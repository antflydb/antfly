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

//! Receiver-local census of surviving native/base-vector effects. The primary
//! row belongs to the exact-input obligation; generated assets, unit scopes and
//! index projections retain their own separate completion requirements.
const platform = @import("antfly_platform");
const std = @import("std");

const publication = @import("artifact_publication.zig");
const inventory = @import("artifact_inventory.zig");
const checkpoint = @import("artifact_stream_checkpoint.zig");
const Observation = @import("artifact_stream_observation.zig").Observation;
const Plan = @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot;
const keys = @import("../internal_keys.zig");
const prefix = "\x00\x00__artifact_publication__:native-stream:";
pub const Key = [prefix.len + 80]u8;
pub const Limits = @import("artifact_stream_census.zig").Limits;

fn key(observation: Observation, root: u128) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..][0..24], &observation.authority.namespace);
    std.mem.writeInt(u64, result[prefix.len + 24 ..][0..8], observation.authority.epoch, .big);
    std.mem.writeInt(u128, result[prefix.len + 32 ..][0..16], root, .big);
    @memcpy(result[prefix.len + 48 ..], &observation.document_digest);
    return result;
}

fn digest(selected: *const Key, bytes: []const u8) publication.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:native-stream:v1:");
    hash.update(selected);
    hash.update(bytes);
    var result: publication.Digest = undefined;
    hash.final(&result);
    return result;
}

const Loaded = struct { requirement: publication.Digest, state: checkpoint.State, digest: publication.Digest, bytes: usize };
fn load(txn: anytype, selected: *const Key) !?Loaded {
    const raw = txn.get(selected) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len < 310 or raw.len > 310 + 2 * checkpoint.max_cursor_bytes or !std.mem.eql(u8, raw[0..4], "ANS1") or
        !std.mem.eql(u8, raw[raw.len - 32 ..], &digest(selected, raw[0 .. raw.len - 32]))) return error.ArtifactCatalogCorrupt;
    const parsed = try checkpoint.decode(raw[36 .. raw.len - 32]);
    if (parsed.state.logical_scan_cursor.len != 0 or !std.mem.eql(u8, selected, &key(parsed.state.observation, parsed.state.root_incarnation))) return error.ArtifactCatalogCorrupt;
    return .{ .requirement = raw[4..36].*, .state = parsed.state, .digest = raw[raw.len - 32 ..][0..32].*, .bytes = raw.len };
}

fn encode(alloc: std.mem.Allocator, selected: *const Key, requirement: publication.Digest, state: checkpoint.State) ![]u8 {
    const inner = try state.encodeAlloc(alloc);
    defer alloc.free(inner);
    const raw = try alloc.alloc(u8, 68 + inner.len);
    @memcpy(raw[0..4], "ANS1");
    @memcpy(raw[4..36], &requirement);
    @memcpy(raw[36 .. raw.len - 32], inner);
    @memcpy(raw[raw.len - 32 ..], &digest(selected, raw[0 .. raw.len - 32]));
    return raw;
}

fn authorize(alloc: std.mem.Allocator, txn: anytype, plan: *const Plan) !publication.Authority {
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    const owner = (try @import("../source_authority.zig").load(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.mem.eql(u8, &owner.namespace, &active.namespace)) return error.ArtifactCatalogDrift;
    var ordered = (try inventory.load(alloc, txn)) orelse return error.ArtifactCatalogDrift;
    defer ordered.deinit();
    const command = ordered.value.command;
    if (command.binding.effect_protocol != 15 or command.binding.epoch != active.epoch or
        !std.mem.eql(u8, &command.binding.digest, &active.catalog_digest) or !std.mem.eql(u8, &command.namespace, &active.namespace)) return error.ArtifactCatalogDrift;
    const catalogs = try inventory.catalogs(txn);
    if (!std.mem.eql(u8, &catalogs.digest(), &active.catalog_digest) or !plan.matchesArtifactInventory(catalogs)) return error.ArtifactCatalogDrift;
    return active;
}

fn catalogStamp(txn: anytype) !?[40]u8 {
    const raw = txn.get(inventory.local_key) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != 40) return error.ArtifactCatalogCorrupt;
    return raw[0..40].*;
}

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    root: u128,
    document: []const u8,
    selected: Key,
    expected: ?publication.Digest,
    catalog_stamp: ?[40]u8,
    observation: Observation,
    encoded: []const u8,
    closed: bool,
    visits: usize,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn stage(self: *const Prepared, txn: anytype, root: u128) !bool {
        if (root == 0 or root != self.root) return error.DurableRootIncarnationUnavailable;
        try self.observation.requireCurrent(txn, self.document);
        if (!std.meta.eql(self.catalog_stamp, try catalogStamp(txn))) return error.ArtifactCatalogDrift;
        const current = try load(txn, &self.selected);
        const actual: ?publication.Digest = if (current) |value| value.digest else null;
        if (!std.meta.eql(actual, self.expected)) return error.EnrichmentSourceChanged;
        try txn.put(&self.selected, self.encoded);
        return self.closed;
    }
};
pub const Step = union(enum) { closed, pending, page: Prepared };

/// One immutable transient snapshot, one physical prefix cursor, at most one
/// primary-row hash and bounded metadata witnesses. Physical cursor values
/// stay unmaterialized: neither vector bodies nor external blob payloads are
/// fetched to infer authorship. Unrecognized/imported flags grant no evidence.
pub fn prepare(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, plan: *const Plan, limits: Limits) !Step {
    try limits.validate();
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    if (document.len == 0 or document.len > checkpoint.max_cursor_bytes) return error.InvalidBatchRequest;
    _ = try authorize(alloc, txn, plan);
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const requirement = (try completion.nativeEffects()).id;
    const observation = try Observation.capture(txn, document);
    const selected = key(observation, root);
    const previous = try load(txn, &selected);
    var state: checkpoint.State = .{ .root_incarnation = root, .observation = observation };
    if (previous) |stored| {
        if (!std.mem.eql(u8, &stored.requirement, &requirement)) return error.ArtifactCatalogDrift;
        const current = blk: {
            stored.state.observation.requireCurrent(txn, document) catch |err| switch (err) {
                error.EnrichmentSourceChanged => break :blk false,
                else => return err,
            };
            break :blk true;
        };
        if (current) {
            if (stored.state.enumerated) return .closed;
            state = stored.state;
        }
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    var transferred = false;
    defer if (!transferred) arena.deinit();
    const owned = arena.allocator();
    const stamp = try catalogStamp(txn);
    const range = try keys.artifactTypePrefixAlloc(owned, document, "embedding");
    if (range.len > checkpoint.max_cursor_bytes) return error.ResourceBudgetExceeded;
    if ((state.cursor.len != 0 and !std.mem.startsWith(u8, state.cursor, range)) or
        (state.scan_cursor.len != 0 and !std.mem.startsWith(u8, state.scan_cursor, range))) return error.ArtifactCatalogCorrupt;
    if (state.cursor.len != 0 and state.scan_cursor.len != 0 and std.mem.order(u8, state.scan_cursor, state.cursor) != .gt) return error.ArtifactCatalogCorrupt;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    const upper = try keys.nextPrefixAlloc(owned, range);
    cursor.setUpperBound(upper);
    const start = if (state.scan_cursor.len != 0) state.scan_cursor else if (state.cursor.len != 0) state.cursor else range;
    var entry = try cursor.seekAtOrAfter(start);
    if (state.scan_cursor.len == 0 and state.cursor.len != 0) if (entry) |item| if (std.mem.eql(u8, item.key, state.cursor)) {
        entry = try cursor.next();
    };
    var budget: @import("artifact_scan_budget.zig").Budget = .{ .max_visits = limits.visits, .max_bytes = limits.bytes, .deadline_ns = platform.time.monotonicNs() +| 2 * std.time.ns_per_ms };
    var source: ?publication.Source = null;
    var advanced = false;
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, range)) {
            entry = null;
            break;
        }
        if (item.key.len > checkpoint.max_cursor_bytes) return error.ResourceBudgetExceeded;
        if (budget.exhausted()) break;
        budget.visit(item.key.len +| item.value.len);
        if (keys.isEmbeddingArtifactKey(item.key)) {
            if (source == null) source = publication.capturePrimarySource(owned, txn, observation.authority.namespace, document) catch |err| switch (err) {
                error.EnrichmentSourceChanged => try publication.capturePrimaryTombstoneSource(owned, txn, observation.authority.namespace, document),
                else => return err,
            };
            // A surviving vector under a tombstone still needs retirement.
            if (!source.?.exists) break;
            var accepted_digest: ?publication.Digest = null;
            if (try @import("artifact_authored_acceptance.zig").readCurrent(txn, root, document, item.key)) |accepted| {
                if (!std.meta.eql(accepted.source.input_position, source.?.input_position) or accepted.source.timestamp != source.?.timestamp or
                    !std.mem.eql(u8, &accepted.source.content_digest, &source.?.content_digest)) return error.EnrichmentSourceChanged;
                accepted_digest = accepted.output_digest;
            } else {
                const provenance = @import("artifact_producer_provenance.zig");
                var proof = provenance.readCurrentArtifactMetadata(alloc, txn, item.key) catch |err| switch (err) {
                    error.EnrichmentSourceChanged => null,
                    else => return err,
                };
                defer if (proof) |*value| value.deinit();
                if (proof) |value| {
                    const effect = for (value.proof.effects) |candidate| {
                        if (std.mem.eql(u8, candidate.key, item.key)) break candidate;
                    } else return error.ArtifactCatalogCorrupt;
                    if (effect.source_index >= value.proof.sources.len or !std.mem.eql(u8, value.proof.sources[effect.source_index].document_key, document)) return error.ArtifactCatalogCorrupt;
                    if (effect.value_digest != null) {
                        var proof_observation = observation;
                        try proof_observation.observeProof(document, value.proof);
                        try state.observation.merge(proof_observation);
                        accepted_digest = value.proof.publication_digest;
                    }
                }
            }
            const receipt = accepted_digest orelse break;
            try state.append(try owned.dupe(u8, item.key), receipt);
        }
        advanced = true;
        entry = try cursor.next();
    }
    if (entry) |item| {
        if (!advanced) {
            return .pending;
        }
        state.scan_cursor = try owned.dupe(u8, item.key);
    } else {
        state.enumerated = true;
        state.scan_cursor = "";
    }
    try state.observation.requireCurrent(txn, document);
    const encoded = try encode(owned, &selected, requirement, state);
    const name = try owned.dupe(u8, document);
    transferred = true;
    return .{ .page = .{ .arena = arena, .root = root, .document = name, .selected = selected, .expected = if (previous) |value| value.digest else null, .catalog_stamp = stamp, .observation = state.observation, .encoded = encoded, .closed = state.enumerated, .visits = budget.visits } };
}

pub const Closure = struct {
    arena: std.heap.ArenaAllocator,
    root: u128,
    requirement: publication.Digest,
    observation: Observation,
    document: []const u8,
    selected: Key,
    record_digest: publication.Digest,
    record_bytes: usize,
    pub fn deinit(self: *Closure) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn requireCurrent(self: Closure, txn: anytype, root: u128) !void {
        if (root == 0 or root != self.root) return error.DurableRootIncarnationUnavailable;
        try self.observation.requireCurrent(txn, self.document);
        const raw = txn.get(&self.selected) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (raw.len != self.record_bytes or raw.len < 32 or !std.mem.eql(u8, raw[raw.len - 32 ..], &self.record_digest)) return error.EnrichmentSourceChanged;
    }
};

pub fn prepareClosure(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, plan: *const Plan) !Closure {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    if (document.len == 0 or document.len > checkpoint.max_cursor_bytes) return error.InvalidBatchRequest;
    _ = try authorize(alloc, txn, plan);
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const requirement = (try completion.nativeEffects()).id;
    const observation = try Observation.capture(txn, document);
    const selected = key(observation, root);
    const stored = (try load(txn, &selected)) orelse return error.ArtifactPublicationPending;
    if (!stored.state.enumerated or !std.mem.eql(u8, &stored.requirement, &requirement)) return error.ArtifactPublicationPending;
    try stored.state.observation.requireCurrent(txn, document);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const name = try arena.allocator().dupe(u8, document);
    return .{ .arena = arena, .root = root, .requirement = requirement, .observation = stored.state.observation, .document = name, .selected = selected, .record_digest = stored.digest, .record_bytes = stored.bytes };
}

pub const Advance = enum { closed, pending, progress };

/// Local verification is not document discharge. Only the all-required
/// ordered completion path may consume the resulting closure handle.
/// Progress requests another bounded scheduling turn; pending waits for new
/// evidence, so a missing certificate cannot spin the maintenance worker.
pub fn advance(alloc: std.mem.Allocator, store: anytype, root: u128, document: []const u8, plan: *const Plan) !Advance {
    var step = blk: {
        var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        break :blk try prepare(alloc, &read, root, document, plan, .{});
    };
    switch (step) {
        .closed => return .closed,
        .pending => return .pending,
        .page => |*page| {
            defer page.deinit();
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            const closed = try page.stage(&txn, root);
            try txn.commit();
            return if (closed) .closed else .progress;
        },
    }
}

pub fn collectObsoletePage(alloc: std.mem.Allocator, store: anytype, root: u128) !bool {
    if (root == 0) return true;
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    return @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store, prefix, 48, 48, &identity);
}

test "ordered artifact inventory native census record binds bytes and physical root" {
    const alloc = std.testing.allocator;
    const observation: Observation = .{ .authority = .{ .namespace = @splat(1), .epoch = 2, .catalog_digest = @splat(3) }, .document_digest = @splat(4), .revision = .{ .raft = .{ .term = 5, .index = 6 } }, .validation_epoch = 7 };
    const selected = key(observation, 41);
    const state: checkpoint.State = .{ .root_incarnation = 41, .observation = observation, .enumerated = true };
    const raw = try encode(alloc, &selected, @splat(8), state);
    defer alloc.free(raw);
    const Probe = struct {
        raw: []const u8,
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.raw;
        }
    };
    var probe: Probe = .{ .raw = raw };
    const loaded = (try load(&probe, &selected)).?;
    try std.testing.expectEqualDeep(state, loaded.state);
    for (0..raw.len) |index| {
        raw[index] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, load(&probe, &selected));
        raw[index] ^= 1;
    }
    const other_root = key(observation, 42);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, load(&probe, &other_root));
    // Rehashing an imported envelope cannot adopt another physical root.
    @memcpy(raw[raw.len - 32 ..], &digest(&other_root, raw[0 .. raw.len - 32]));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, load(&probe, &other_root));
    probe.raw = raw[0 .. raw.len - 1];
    try std.testing.expectError(error.ArtifactCatalogCorrupt, load(&probe, &selected));
}

test "ordered artifact inventory native census resumes authored inventory and rejects imported flags" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const docstore = @import("../docstore.zig");
    const authored = @import("artifact_authored_acceptance.zig");
    const obligations = @import("artifact_producer_obligations.zig");
    const completion = @import("artifact_completion_progress.zig");
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native-census", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 7, .shard_id = 11, .range_id = 14 }, .online_source_authority = .native, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try db_mod.DB.open(alloc, path, options);
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    const authority: publication.Authority = .{ .namespace = catalog.namespace, .epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest };
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try inventory.stageOrdered(alloc, &txn, catalog, 1);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try obligations.begin(alloc, &txn, authority);
        try @import("artifact_producer_validation.zig").begin(alloc, &txn, authority);
        try txn.commit();
    }
    const document = "doc\x00\xff";
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const primary = try keys.documentKeyAlloc(owned, document);
    const ttl = try keys.ttlKeyAlloc(owned, document);
    const codec = @import("enrichment/artifact_codec.zig");
    const dense = try codec.encodeAuthoredDenseEmbeddingAlloc(owned, &.{ 1, 2 });
    const sparse = try codec.encodeAuthoredSparseEmbeddingAlloc(owned, &.{1}, &.{2});
    const writes = try owned.alloc(docstore.KVPair, 132);
    writes[0] = .{ .key = primary, .value = "{}" };
    writes[1] = .{ .key = ttl, .value = &.{ 1, 0, 0, 0, 0, 0, 0, 0 } };
    for (writes[2..], 0..) |*write, index| write.* = .{ .key = try keys.embeddingArtifactKeyForDocumentAlloc(owned, document, try std.fmt.allocPrint(owned, "vector-{d:0>3}", .{index})), .value = if (index % 2 == 0) dense else sparse };
    var ingress = try authored.Prepared.init(alloc, db.root_incarnation, writes[2..], writes);
    defer ingress.deinit();
    try db.core.store.putBatchWithReplayAndParticipant(null, writes, &.{}, null, .{}, ingress.participant());
    var pin = try db.core.index_manager.acquireWritePlanSnapshot();
    var pin_held = true;
    defer if (pin_held) pin.release();
    try std.testing.expectEqual(@as(usize, 0), pin.plan().generated_templates.len);
    var first = blk: {
        var read = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const Probe = struct {
            parent: *@TypeOf(read),
            pub const CursorAdapter = @TypeOf(read).CursorAdapter;
            pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
                try std.testing.expect(!keys.isEmbeddingArtifactKey(selected));
                return self.parent.get(selected);
            }
            pub fn openPhysicalCursorAdapter(self: *@This()) !CursorAdapter {
                return self.parent.openPhysicalCursorAdapter();
            }
        };
        var probe: Probe = .{ .parent = &read };
        try std.testing.expectError(error.ArtifactPublicationPending, prepareClosure(alloc, &probe, db.root_incarnation, document, pin.plan()));
        break :blk (try prepare(alloc, &probe, db.root_incarnation, document, pin.plan(), .{ .visits = 17 })).page;
    };
    defer first.deinit();
    try std.testing.expect(first.visits > 0 and first.visits <= 17 and !first.closed);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const Check = struct {
            fn run(a: std.mem.Allocator, txn: *@TypeOf(read), root: u128, snapshot: *const Plan) !void {
                var step = try prepare(a, txn, root, "doc\x00\xff", snapshot, .{ .visits = 1, .bytes = 1 });
                defer if (step == .page) step.page.deinit();
                try std.testing.expect(step == .page);
                try std.testing.expectEqual(@as(usize, 1), step.page.visits);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ &read, db.root_incarnation, pin.plan() });
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.DurableRootIncarnationUnavailable, first.stage(&txn, db.root_incarnation + 1));
        try std.testing.expect(!try first.stage(&txn, db.root_incarnation));
    }
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try load(&read, &first.selected)) == null);
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(!try first.stage(&txn, db.root_incarnation));
        try txn.commit();
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, first.stage(&txn, db.root_incarnation));
    }
    pin.release();
    pin_held = false;
    db.close();
    db = try db_mod.DB.open(alloc, path, options);
    pin = try db.core.index_manager.acquireWritePlanSnapshot();
    pin_held = true;
    var closed = false;
    for (0..256) |_| {
        if (try advance(alloc, db.core.store, db.root_incarnation, document, pin.plan()) == .closed) {
            closed = true;
            break;
        }
    }
    try std.testing.expect(closed);
    var closure = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 130), (try load(&read, &first.selected)).?.state.members);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
        try std.testing.expect((try prepare(alloc, &read, db.root_incarnation, document, pin.plan(), .{})) == .closed);
        break :blk try prepareClosure(alloc, &read, db.root_incarnation, document, pin.plan());
    };
    defer closure.deinit();
    const original_position = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        break :blk (try obligations.lookupWork(alloc, &read, authority, document)).?.position;
    };
    {
        var verifier: completion.StreamVerifier = .{ .plan = pin.plan() };
        var page = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk (try completion.prepare(alloc, &read, db.root_incarnation, &pin.plan().completion_plan.?, document, &verifier, .{})).?;
        };
        defer page.deinit();
        try std.testing.expect(page.atEnd());
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try page.stage(&txn, db.root_incarnation, &pin.plan().completion_plan.?));
        try txn.commit();
    }
    const imported = try keys.embeddingArtifactKeyForDocumentAlloc(owned, document, "zz-imported");
    try db.core.store.put(imported, dense);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, closure.requireCurrent(&read, db.root_incarnation));
        // Artifact-only membership changes reopen completed work without
        // lying about the unchanged primary row's source position.
        const work = (try obligations.lookupWork(alloc, &read, authority, document)).?;
        try std.testing.expectEqualDeep(original_position, work.position);
        try std.testing.expect(work.revision > 1);
        try std.testing.expectEqual(@as(u64, 1), (try obligations.load(&read)).?.pending_documents);
    }
    var pending = false;
    for (0..256) |_| {
        var step = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk try prepare(alloc, &read, db.root_incarnation, document, pin.plan(), .{ .visits = 17 });
        };
        switch (step) {
            .closed => return error.TestUnexpectedResult,
            .pending => {
                pending = true;
                break;
            },
            .page => |*page| {
                defer page.deinit();
                var txn = try db.core.store.beginWriteTxn();
                errdefer txn.abort();
                try std.testing.expect(!try page.stage(&txn, db.root_incarnation));
                try txn.commit();
            },
        }
    }
    try std.testing.expect(pending);
    try db.core.store.delete(imported);
    closed = false;
    for (0..256) |_| {
        if (try advance(alloc, db.core.store, db.root_incarnation, document, pin.plan()) == .closed) {
            closed = true;
            break;
        }
    }
    try std.testing.expect(closed);
}
