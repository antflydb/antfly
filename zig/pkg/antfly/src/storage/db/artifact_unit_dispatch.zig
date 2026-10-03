// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Generation-bound producer discovery, separate from receiver verification.
//! A proposed cursor may advance only with durable admission of its selected
//! jobs. Neither an empty page nor scan exhaustion discharges an obligation.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const chunks = @import("artifact_chunk_publication.zig");
const census = @import("artifact_stream_census.zig");
const Digest = publication.Digest;
const max_cursor_bytes = @import("artifact_stream_checkpoint.zig").max_cursor_bytes;

pub const Phase = enum(u8) { desired, retiring };
pub const Cursor = struct {
    identity: Digest,
    generation: Digest,
    phase: Phase = .desired,
    ordinal: u32 = 0,
    retirement_key: []const u8 = "",

    pub fn validate(self: Cursor) !void {
        if (std.mem.allEqual(u8, &self.generation, 0) or self.retirement_key.len > max_cursor_bytes or
            (self.phase == .desired and self.retirement_key.len != 0) or
            (self.phase == .retiring and self.ordinal != 0)) return error.InvalidBatchRequest;
    }

    pub fn encodeAlloc(self: Cursor, alloc: std.mem.Allocator) ![]u8 {
        try self.validate();
        const raw = try alloc.alloc(u8, 109 + self.retirement_key.len);
        @memcpy(raw[0..4], "AUD1");
        @memcpy(raw[4..36], &self.identity);
        @memcpy(raw[36..68], &self.generation);
        raw[68] = @backingInt(self.phase);
        std.mem.writeInt(u32, raw[69..73], self.ordinal, .little);
        std.mem.writeInt(u32, raw[73..77], @intCast(self.retirement_key.len), .little);
        @memcpy(raw[77 .. raw.len - 32], self.retirement_key);
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
        return raw;
    }

    /// Borrows raw; the caller owns its durable cursor bytes.
    pub fn decode(raw: []const u8) !Cursor {
        if (raw.len < 109 or raw.len > 109 + max_cursor_bytes or !std.mem.eql(u8, raw[0..4], "AUD1") or
            std.mem.readInt(u32, raw[73..77], .little) != raw.len - 109) return error.ArtifactCatalogCorrupt;
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
        const result: Cursor = .{ .identity = raw[4..36].*, .generation = raw[36..68].*, .phase = std.enums.fromInt(Phase, raw[68]) orelse return error.ArtifactCatalogCorrupt, .ordinal = std.mem.readInt(u32, raw[69..73], .little), .retirement_key = raw[77 .. raw.len - 32] };
        result.validate() catch return error.ArtifactCatalogCorrupt;
        return result;
    }
};

pub fn scopeIdentity(session: anytype) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:unit-producer-discovery:v1:");
    hash.update(&session.authority.namespace);
    hash.update(&session.authority.catalog_digest);
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, session.authority.epoch, .little);
    hash.update(&number);
    for ([_][]const u8{ session.request.doc_key, session.request.upstream_artifact_name, session.request.artifact_name }) |name| {
        std.mem.writeInt(u64, &number, name.len, .little);
        hash.update(&number);
        hash.update(name);
    }
    return hash.finalResult();
}

pub const Page = struct {
    source: union(Phase) { desired: chunks.UnitPage, retiring: chunks.RetirementPage },
    /// Owned logical parent-unit keys needing production or empty replacement.
    /// Consumers must retain child identity AND generation when admitting work.
    missing: []const []const u8,
    document: []const u8,
    child: []const u8,
    observation: @import("artifact_stream_observation.zig").Observation,
    before: ?Cursor,
    after: Cursor,
    visited: u32,
    /// End of this discovery sweep, never a completion certificate. Subsequent
    /// sweeps must wrap to find scopes inserted behind a retirement cursor.
    at_end: bool,

    pub fn deinit(self: *Page) void {
        switch (self.source) {
            inline else => |*value| value.deinit(),
        }
        self.* = undefined;
    }

    /// Point-only admission fence. Concurrent child writes may invalidate a
    /// proposal, but never rewind the durable generation-bound discovery
    /// cursor. This check is not acceptance of any producer output.
    pub fn requireCurrent(self: *const Page, txn: anytype) !void {
        try self.observation.requireCurrent(txn, self.document);
        switch (self.source) {
            inline else => |value| {
                const current = txn.get(value.head_key) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
                if (!std.mem.eql(u8, current, value.head_value) or
                    !std.meta.eql(value.head_position, try publication.artifactRevision(txn, self.observation.authority.namespace, value.head_key))) return error.EnrichmentSourceChanged;
            },
        }
    }
};

fn ownCursor(alloc: std.mem.Allocator, cursor: ?Cursor) !?Cursor {
    var result = cursor orelse return null;
    result.retirement_key = try alloc.dupe(u8, result.retirement_key);
    return result;
}

/// The caller supplies a catalog-authorized verification session and retains
/// its snapshot through discovery. Output owns all keys/cursor data. Child
/// writes do not reset this cursor: the immutable parent generation does.
pub fn prepare(alloc: std.mem.Allocator, session: anytype, after: ?Cursor, limits: census.Limits) !Page {
    try limits.validate();
    const bound = scopeIdentity(session);
    if (after) |cursor| {
        try cursor.validate();
        if (!std.mem.eql(u8, &cursor.identity, &bound)) return error.ArtifactCatalogDrift;
    }
    const phase = if (after) |cursor| cursor.phase else .desired;
    switch (phase) {
        .desired => {
            var page = try session.unitPage(alloc, if (after) |cursor| .{ .generation = cursor.generation, .next_ordinal = cursor.ordinal } else null, @intCast(limits.visits), limits.bytes);
            errdefer page.deinit();
            const pending = try page.pendingChildren(alloc, session.txn, session.request.doc_key, session.request.artifact_name);
            const document = try page.arena.allocator().dupe(u8, session.request.doc_key);
            const child = try page.arena.allocator().dupe(u8, session.request.artifact_name);
            const before = try ownCursor(page.arena.allocator(), after);
            return .{ .source = .{ .desired = page }, .missing = pending.units, .document = document, .child = child, .observation = pending.observation, .before = before, .after = .{ .identity = bound, .generation = page.after.generation, .phase = if (page.at_end) .retiring else .desired, .ordinal = if (page.at_end) 0 else page.after.next_ordinal }, .visited = page.visited, .at_end = false };
        },
        .retiring => {
            const cursor = after.?;
            var page = try session.retirementPage(alloc, .{ .generation = cursor.generation, .after_key = cursor.retirement_key }, @intCast(limits.visits), limits.bytes);
            errdefer page.deinit();
            const pending = try page.pendingChildren(alloc, session.txn, session.request.doc_key, session.request.artifact_name);
            const document = try page.arena.allocator().dupe(u8, session.request.doc_key);
            const child = try page.arena.allocator().dupe(u8, session.request.artifact_name);
            const before = try ownCursor(page.arena.allocator(), after);
            return .{ .source = .{ .retiring = page }, .missing = pending.units, .document = document, .child = child, .observation = pending.observation, .before = before, .after = .{ .identity = bound, .generation = page.after.generation, .phase = .retiring, .retirement_key = page.after.after_key }, .visited = page.visited, .at_end = page.at_end };
        },
    }
}

/// Resolve the executable parent from the immutable child requirement; callers
/// cannot supply provider settings or silently drop a configured child whose
/// parent is not executable yet.
pub fn discover(alloc: std.mem.Allocator, txn: anytype, document: []const u8, child: []const u8, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, after: ?Cursor, limits: census.Limits) !Page {
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const node = try completion.unitChild(child);
    const ordinal = node.parent_template orelse return error.ArtifactPublicationPending;
    if (ordinal >= plan.generated_templates.len) return error.ArtifactCatalogDrift;
    var parent = plan.generated_templates[ordinal];
    parent.doc_key = document;
    const session = (try chunks.unitVerificationSession(alloc, txn, parent, child, plan)) orelse return error.ArtifactCatalogDrift;
    return prepare(alloc, session, after, limits);
}

test "ordered artifact inventory unit dispatch cursor binds phase and exact framing" {
    const alloc = std.testing.allocator;
    const cursor: Cursor = .{ .identity = @splat(1), .generation = @splat(2), .phase = .retiring, .retirement_key = "binary\x00\xff" };
    const raw = try cursor.encodeAlloc(alloc);
    defer alloc.free(raw);
    try std.testing.expectEqualDeep(cursor, try Cursor.decode(raw));
    for (0..raw.len) |length| try std.testing.expectError(error.ArtifactCatalogCorrupt, Cursor.decode(raw[0..length]));
    raw[40] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Cursor.decode(raw));
    raw[40] ^= 1;
    raw[68] = 255;
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Cursor.decode(raw));
    var invalid = cursor;
    invalid.phase = .desired;
    try std.testing.expectError(error.InvalidBatchRequest, invalid.encodeAlloc(alloc));
    invalid = cursor;
    invalid.ordinal = 1;
    try std.testing.expectError(error.InvalidBatchRequest, invalid.encodeAlloc(alloc));
}
