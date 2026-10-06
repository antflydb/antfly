// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Receiver-verified reconciliation of one extraction producer's unit child.
//! Discovery is not progress: every returned child must have a current accepted
//! replacement before a page can advance. This registry closes that child only,
//! never the document's other producers, projections, or replay obligation.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const Observation = @import("artifact_stream_observation.zig").Observation;
const checkpoints = @import("artifact_stream_checkpoint.zig");
const census = @import("artifact_stream_census.zig");
const prefix = "\x00\x00__artifact_publication__:unit-progress:";
const Key = [prefix.len + 80]u8;
pub const Phase = enum(u8) { desired, retiring, complete };
pub const Progress = struct {
    observation: Observation,
    generation: publication.Digest = @splat(0),
    phase: Phase = .desired,
    desired_ordinal: u32 = 0,
    retirement_cursor: []const u8 = "",
    verified_units: u64 = 0,
    chain: publication.Digest = @splat(0),
};
const Record = struct { root: u128, progress: Progress, claim: census.Claim };
const Stamp = struct { bytes: usize, digest: publication.Digest };
const Loaded = struct { record: Record, stamp: Stamp };

fn key(root: u128, session: anytype) !Key {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    const request = session.request;
    if (request.kind != .chunk_text or request.artifact_name.len == 0 or request.upstream_artifact_name.len == 0) return error.InvalidBatchRequest;
    const local = checkpoints.key(session.authority, request.doc_key, .{ .root_incarnation = root, .kind = .enrichment, .name = request.artifact_name, .generation = session.authority.epoch, .artifact = request.upstream_artifact_name });
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], local[local.len - 80 ..]);
    return result;
}

fn header(record: Record) ![303]u8 {
    const p = record.progress;
    if (p.retirement_cursor.len > checkpoints.max_cursor_bytes) return error.InvalidBatchRequest;
    var raw: [303]u8 = @splat(0);
    @memcpy(raw[0..4], "AUP1");
    std.mem.writeInt(u128, raw[4..20], record.root, .little);
    @memcpy(raw[20..44], &p.observation.authority.namespace);
    std.mem.writeInt(u64, raw[44..52], p.observation.authority.epoch, .little);
    @memcpy(raw[52..84], &p.observation.authority.catalog_digest);
    @memcpy(raw[84..116], &p.observation.document_digest);
    if (p.observation.revision) |position| @memcpy(raw[116..149], &try position.encode());
    std.mem.writeInt(u64, raw[149..157], p.observation.validation_epoch, .little);
    raw[157] = @intFromBool(p.observation.foreign_inputs);
    @memcpy(raw[158..190], &p.generation);
    raw[190] = @backingInt(p.phase);
    std.mem.writeInt(u32, raw[191..195], p.desired_ordinal, .little);
    std.mem.writeInt(u64, raw[195..203], p.verified_units, .little);
    @memcpy(raw[203..235], &p.chain);
    @memcpy(raw[235..267], &record.claim.before);
    @memcpy(raw[267..299], &record.claim.after);
    std.mem.writeInt(u32, raw[299..303], @intCast(p.retirement_cursor.len), .little);
    return raw;
}

/// Portable across physical roots, but bound to the authorized parent/child
/// relationship and exact causal cut. Unrelated writes do not disturb a purely
/// owner-local prefix; foreign inputs make the validation epoch significant.
fn fingerprint(selected: *const Key, progress: Progress) !publication.Digest {
    var raw = try header(.{ .root = 0, .progress = progress, .claim = .{ .before = @splat(0), .after = @splat(0) } });
    if (!progress.observation.foreign_inputs) @memset(raw[149..157], 0);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:unit-progress-claim:v1:");
    hash.update(selected[selected.len - 32 ..]);
    hash.update(raw[20..235]);
    hash.update(raw[299..303]);
    hash.update(progress.retirement_cursor);
    return hash.finalResult();
}

fn checksum(selected: *const Key, raw: []const u8) publication.Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:unit-progress-record:v1:");
    hash.update(selected);
    hash.update(raw);
    return hash.finalResult();
}

fn encodeAlloc(alloc: std.mem.Allocator, selected: *const Key, record: Record) ![]u8 {
    const fixed = try header(record);
    const raw = try alloc.alloc(u8, 335 + record.progress.retirement_cursor.len);
    errdefer alloc.free(raw);
    @memcpy(raw[0..303], &fixed);
    @memcpy(raw[303 .. raw.len - 32], record.progress.retirement_cursor);
    @memcpy(raw[raw.len - 32 ..], &checksum(selected, raw[0 .. raw.len - 32]));
    _ = try decode(selected, raw);
    return raw;
}

fn decode(selected: *const Key, raw: []const u8) !Loaded {
    if (raw.len < 335 or raw.len > 335 + checkpoints.max_cursor_bytes or !std.mem.eql(u8, raw[0..4], "AUP1") or
        raw[157] > 1 or raw[190] > @backingInt(Phase.complete) or
        std.mem.readInt(u32, raw[299..303], .little) != raw.len - 335 or
        !std.mem.eql(u8, raw[raw.len - 32 ..], &checksum(selected, raw[0 .. raw.len - 32]))) return error.ArtifactCatalogCorrupt;
    const p: Progress = .{
        .observation = .{ .authority = .{ .namespace = raw[20..44].*, .epoch = std.mem.readInt(u64, raw[44..52], .little), .catalog_digest = raw[52..84].* }, .document_digest = raw[84..116].*, .revision = if (std.mem.allEqual(u8, raw[116..149], 0)) null else publication.Position.decode(raw[116..149]) catch return error.ArtifactCatalogCorrupt, .validation_epoch = std.mem.readInt(u64, raw[149..157], .little), .foreign_inputs = raw[157] == 1 },
        .generation = raw[158..190].*,
        .phase = @fromBackingInt(raw[190]),
        .desired_ordinal = std.mem.readInt(u32, raw[191..195], .little),
        .verified_units = std.mem.readInt(u64, raw[195..203], .little),
        .chain = raw[203..235].*,
        .retirement_cursor = raw[303 .. raw.len - 32],
    };
    const record: Record = .{ .root = std.mem.readInt(u128, raw[4..20], .little), .progress = p, .claim = .{ .before = raw[235..267].*, .after = raw[267..299].* } };
    if (record.root == 0 or p.observation.authority.epoch == 0 or std.mem.allEqual(u8, &p.generation, 0) or
        (p.phase != .retiring and p.retirement_cursor.len != 0) or
        (p.verified_units == 0) != std.mem.allEqual(u8, &p.chain, 0) or
        !std.mem.eql(u8, selected[0..prefix.len], prefix) or
        !std.mem.eql(u8, selected[prefix.len..][0..24], &p.observation.authority.namespace) or
        std.mem.readInt(u64, selected[prefix.len + 24 ..][0..8], .big) != p.observation.authority.epoch or
        std.mem.readInt(u128, selected[prefix.len + 32 ..][0..16], .big) != record.root or
        !std.mem.eql(u8, &record.claim.after, &try fingerprint(selected, p)) or
        std.meta.eql(record.claim.before, record.claim.after)) return error.ArtifactCatalogCorrupt;
    return .{ .record = record, .stamp = .{ .bytes = raw.len, .digest = raw[raw.len - 32 ..][0..32].* } };
}

fn load(txn: anytype, selected: *const Key) !?Loaded {
    const raw = txn.get(selected) catch |err| if (err == error.NotFound) return null else return err;
    return try decode(selected, raw);
}

fn catalogStamp(txn: anytype) !?[40]u8 {
    const raw = txn.get(@import("artifact_inventory.zig").local_key) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != 40) return error.ArtifactCatalogCorrupt;
    return raw[0..40].*;
}

fn requireStamp(txn: anytype, selected: *const Key, expected: ?Stamp) !void {
    const raw = txn.get(selected) catch |err| if (err == error.NotFound) null else return err;
    if (expected) |value| {
        const current = raw orelse return error.EnrichmentSourceChanged;
        if (current.len != value.bytes or current.len < 32 or !std.mem.eql(u8, current[current.len - 32 ..], &value.digest)) return error.EnrichmentSourceChanged;
    } else if (raw != null) return error.EnrichmentSourceChanged;
}

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    selected: Key,
    expected: ?Stamp,
    catalog_stamp: ?[40]u8,
    encoded: []const u8,
    document: []const u8,
    record: Record,
    duplicate: bool,
    limits: census.Limits,

    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn command(self: *const Prepared, child: []const u8) publication.Command {
        const authority = self.record.progress.observation.authority;
        var result: publication.Command = .{ .mode = .reconcile_units, .producer_kind = .enrichment, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = child, .producer_generation = authority.epoch, .producer_artifact_name = child, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0), .census = .{ .document_key = self.document, .chunk_name = child, .visits = @intCast(self.limits.visits), .bytes = @intCast(self.limits.bytes), .before = self.record.claim.before, .after = self.record.claim.after } };
        result.publication_digest = result.digest();
        return result;
    }

    /// Same owner transaction as the applied marker and durable outbox. All
    /// discovery/proof decoding ran in prepare(); only point fences run here.
    pub fn stage(self: *const Prepared, txn: anytype, root: u128) !bool {
        if (root == 0 or root != self.record.root) return error.DurableRootIncarnationUnavailable;
        if (!std.meta.eql(self.catalog_stamp, try catalogStamp(txn))) return error.ArtifactCatalogDrift;
        try requireStamp(txn, &self.selected, self.expected);
        const state = (try @import("artifact_producer_obligations.zig").load(txn)) orelse return error.ArtifactCatalogDrift;
        try state.requireAuthority(self.record.progress.observation.authority);
        if (state.sealed_attempt != null) return error.RetainedEffectsFenceMismatch;
        try self.record.progress.observation.requireCurrent(txn, self.document);
        if (!self.duplicate) try txn.put(&self.selected, self.encoded);
        return self.record.progress.phase == .complete;
    }
};

fn appendPage(progress: *Progress, page: anytype, verified: @import("artifact_chunk_publication.zig").VerifiedPage) !void {
    try progress.observation.merge(verified.observation);
    progress.generation = page.after.generation;
    if (page.units.len == 0) return;
    progress.verified_units = std.math.add(u64, progress.verified_units, @intCast(page.units.len)) catch return error.ResourceLimitExceeded;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:unit-progress-chain:v1:");
    hash.update(&progress.chain);
    hash.update(&verified.digest);
    var count: [8]u8 = undefined;
    std.mem.writeInt(u64, &count, @intCast(page.units.len), .little);
    hash.update(&count);
    progress.chain = hash.finalResult();
}

/// The session must come from catalog-authorized unitSession(). Never accepts
/// a sender's cursor: the receiver resumes its own durable predecessor, derives
/// the next page, and checks the optional portable before/after claim.
pub fn prepare(alloc: std.mem.Allocator, root: u128, session: anytype, limits: census.Limits, claim: ?census.Claim) !?Prepared {
    try limits.validate();
    const selected = try key(root, session);
    const txn = session.txn;
    const document = session.request.doc_key;
    const child = session.request.artifact_name;
    const current = try Observation.capture(txn, document);
    if (!std.meta.eql(current.authority, session.authority)) return error.ArtifactCatalogDrift;
    const old = try load(txn, &selected);
    var previous: Progress = .{ .observation = current };
    var duplicate = false;
    if (old) |stored| {
        const valid = blk: {
            stored.record.progress.observation.requireCurrent(txn, document) catch |err| switch (err) {
                error.EnrichmentSourceChanged => break :blk false,
                else => return err,
            };
            break :blk true;
        };
        if (valid) {
            previous = stored.record.progress;
            duplicate = if (claim) |expected| std.meta.eql(expected, stored.record.claim) else false;
        }
    }
    if (!duplicate and previous.phase == .complete) {
        if (claim != null) return error.EnrichmentSourceChanged;
        return null;
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var next = previous;
    const before = try fingerprint(&selected, previous);
    if (!duplicate) switch (previous.phase) {
        .desired => {
            const after: ?@import("artifact_extraction_generation.zig").Position = if (std.mem.allEqual(u8, &previous.generation, 0)) null else .{ .generation = previous.generation, .next_ordinal = previous.desired_ordinal };
            var page = try session.unitPage(alloc, after, @intCast(limits.visits), limits.bytes);
            defer page.deinit();
            const verified = try page.verifyChildren(alloc, txn, document, child);
            try appendPage(&next, page, verified);
            next.desired_ordinal = page.after.next_ordinal;
            if (page.at_end) next.phase = .retiring;
        },
        .retiring => {
            var page = try session.retirementPage(alloc, .{ .generation = previous.generation, .after_key = previous.retirement_cursor }, @intCast(limits.visits), limits.bytes);
            defer page.deinit();
            const verified = try page.verifyChildren(alloc, txn, document, child);
            try appendPage(&next, page, verified);
            next.retirement_cursor = if (page.at_end) "" else try owned.dupe(u8, page.after.after_key);
            if (page.at_end) next.phase = .complete;
        },
        .complete => unreachable,
    };
    const actual_claim: census.Claim = if (duplicate) old.?.record.claim else .{ .before = before, .after = try fingerprint(&selected, next) };
    if (claim) |expected| if (!std.meta.eql(expected, actual_claim)) return error.EnrichmentSourceChanged;
    const record: Record = .{ .root = root, .progress = next, .claim = actual_claim };
    const encoded = try encodeAlloc(owned, &selected, record);
    // Decode the owned bytes so no borrowed cursor survives its read snapshot.
    const owned_record = (try decode(&selected, encoded)).record;
    const owned_result_document = try owned.dupe(u8, document);
    return .{ .arena = arena, .selected = selected, .expected = if (old) |value| value.stamp else null, .catalog_stamp = try catalogStamp(txn), .encoded = encoded, .document = owned_result_document, .record = owned_record, .duplicate = duplicate, .limits = limits };
}

pub fn prepareCommand(alloc: std.mem.Allocator, txn: anytype, root: u128, command: publication.Command, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !Prepared {
    try command.validate(alloc);
    if (command.mode != .reconcile_units) return error.InvalidBatchRequest;
    const page = command.census.?;
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const child = try completion.unitChild(command.producer_name);
    const ordinal = child.parent_template orelse return error.ArtifactCatalogDrift;
    if (ordinal >= plan.generated_templates.len) return error.ArtifactCatalogDrift;
    var parent = plan.generated_templates[ordinal];
    parent.doc_key = page.document_key;
    const session = (try @import("artifact_chunk_publication.zig").unitVerificationSession(alloc, txn, parent, command.producer_name, plan)) orelse return error.ArtifactCatalogDrift;
    if (session.authority.epoch != command.authority_epoch or !std.mem.eql(u8, &session.authority.namespace, &command.namespace) or
        !std.mem.eql(u8, &session.authority.catalog_digest, &command.catalog_digest)) return error.ArtifactCatalogDrift;
    return (try prepare(alloc, root, session, .{ .visits = page.visits, .bytes = page.bytes }, .{ .before = page.before, .after = page.after })) orelse error.EnrichmentSourceChanged;
}

pub const Closure = struct {
    alloc: std.mem.Allocator,
    root: u128,
    selected: Key,
    stamp: Stamp,
    catalog_stamp: ?[40]u8,
    document: []u8,
    observation: Observation,

    pub fn deinit(self: *Closure) void {
        self.alloc.free(self.document);
        self.* = undefined;
    }
    pub fn requireCurrent(self: Closure, txn: anytype, root: u128) !void {
        if (root == 0 or root != self.root) return error.DurableRootIncarnationUnavailable;
        if (!std.meta.eql(self.catalog_stamp, try catalogStamp(txn))) return error.ArtifactCatalogDrift;
        try self.observation.requireCurrent(txn, self.document);
        try requireStamp(txn, &self.selected, self.stamp);
    }
};

/// A single child requirement only. An immutable completion plan must combine
/// this with the independent parent, projection and sibling requirements.
pub fn prepareClosure(alloc: std.mem.Allocator, root: u128, session: anytype) !Closure {
    const selected = try key(root, session);
    const stored = (try load(session.txn, &selected)) orelse return error.ArtifactPublicationPending;
    if (stored.record.progress.phase != .complete) return error.ArtifactPublicationPending;
    try stored.record.progress.observation.requireCurrent(session.txn, session.request.doc_key);
    return .{ .alloc = alloc, .root = root, .selected = selected, .stamp = stored.stamp, .catalog_stamp = try catalogStamp(session.txn), .document = try alloc.dupe(u8, session.request.doc_key), .observation = stored.record.progress.observation };
}

pub fn collectObsoletePage(alloc: std.mem.Allocator, store: anytype, root: u128) !bool {
    if (root == 0) return true;
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    return @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store, prefix, 48, 48, &identity);
}

test "ordered artifact inventory unit progress codec binds root scope and canonical causal state" {
    const alloc = std.testing.allocator;
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 2, .catalog_digest = @splat(3) };
    const session = .{ .authority = authority, .request = @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest{ .kind = .chunk_text, .doc_key = "doc", .index_name = "", .artifact_name = "child", .upstream_artifact_name = "parent", .source_field = "body" } };
    const selected = try key(7, session);
    var progress: Progress = .{ .observation = .{ .authority = authority, .document_digest = @splat(4), .revision = .{ .raft = .{ .term = 2, .index = 8 } }, .validation_epoch = 11 }, .generation = @splat(5), .phase = .retiring, .desired_ordinal = 9, .retirement_cursor = "binary\x00\xff", .verified_units = 3, .chain = @splat(6) };
    const record: Record = .{ .root = 7, .progress = progress, .claim = .{ .before = @splat(9), .after = try fingerprint(&selected, progress) } };
    const raw = try encodeAlloc(alloc, &selected, record);
    defer alloc.free(raw);
    try std.testing.expectEqualDeep(record, (try decode(&selected, raw)).record);
    const foreign_root = try key(8, session);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(&foreign_root, raw));
    // Portable claims exclude the local physical-root key and unrelated epochs.
    try std.testing.expectEqualDeep(try fingerprint(&selected, progress), try fingerprint(&foreign_root, progress));
    progress.observation.validation_epoch += 1;
    try std.testing.expectEqualDeep(record.claim.after, try fingerprint(&selected, progress));
    progress.observation.foreign_inputs = true;
    try std.testing.expect(!std.meta.eql(record.claim.after, try fingerprint(&selected, progress)));
    const foreign_digest = try fingerprint(&selected, progress);
    progress.observation.validation_epoch += 1;
    try std.testing.expect(!std.meta.eql(foreign_digest, try fingerprint(&selected, progress)));
    for (0..raw.len) |i| {
        raw[i] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(&selected, raw));
        raw[i] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, decode(&selected, raw[0..i]));
    }
    var invalid = record;
    invalid.progress.phase = .complete;
    invalid.claim.after = try fingerprint(&selected, invalid.progress);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, encodeAlloc(alloc, &selected, invalid));
    invalid = record;
    invalid.progress.verified_units = 0;
    invalid.claim.after = try fingerprint(&selected, invalid.progress);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, encodeAlloc(alloc, &selected, invalid));
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, k: *const Key, r: Record) !void {
            const bytes = try encodeAlloc(a, k, r);
            defer a.free(bytes);
            _ = try decode(k, bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ &selected, record });
}
