// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Bounded discovery of staged output sets, not producer completion or an
//! ordered progress certificate. No member bodies or snapshots escape a page.
const std = @import("std");
const generations = @import("artifact_chunk_generation.zig");
const chunks = @import("artifact_chunk_manifest.zig");
const publication = @import("artifact_publication.zig");
const keys = @import("../internal_keys.zig");
const time = @import("antfly_platform").time;
const scopes = @import("artifact_generation_scope.zig");

pub const Cursor = struct {
    authority: publication.Authority,
    scope_digest: publication.Digest,
    after: publication.Digest,
    pub const encoded_len = 164;
    pub fn encode(self: Cursor) [encoded_len]u8 {
        var raw: [encoded_len]u8 = undefined;
        @memcpy(raw[0..4], "AGR1");
        @memcpy(raw[4..28], &self.authority.namespace);
        std.mem.writeInt(u64, raw[28..36], self.authority.epoch, .little);
        @memcpy(raw[36..68], &self.authority.catalog_digest);
        @memcpy(raw[68..100], &self.scope_digest);
        @memcpy(raw[100..132], &self.after);
        std.crypto.hash.sha2.Sha256.hash(raw[0..132], raw[132..164], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !Cursor {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], "AGR1")) return error.ArtifactCatalogCorrupt;
        var checksum: publication.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..132], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[132..164])) return error.ArtifactCatalogCorrupt;
        return .{ .authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* }, .scope_digest = raw[68..100].*, .after = raw[100..132].* };
    }
};

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    authority: publication.Authority,
    selected: ?publication.Digest,
    states: []const generations.State,
    next: ?Cursor,
    at_end: bool,
    pub fn deinit(self: *Page) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Limits = struct { rows: usize = 128, bytes: usize = 64 * 1024 };

/// A caller must recheck the state/head in its ordered write transaction.
/// Concurrent begins can land behind this cursor; a fair discovery sweep must
/// wrap at the end, not treat a completed scan as an admission/seal barrier.
pub fn discover(alloc: std.mem.Allocator, store: anytype, scope: []const u8, after: ?Cursor, limits: Limits) !Page {
    if (!scopes.isKey(scope) or scope.len > publication.max_payload_bytes - 33 or limits.rows == 0 or limits.rows > 128 or limits.bytes == 0 or limits.bytes > 64 * 1024) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    const authority = (try publication.authority(&read)) orelse return error.ArtifactCatalogDrift;
    var digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(scope, &digest, .{});
    if (after) |cursor| {
        if (!std.meta.eql(cursor.authority, authority)) return error.ArtifactCatalogDrift;
        if (!std.mem.eql(u8, &cursor.scope_digest, &digest)) return error.InvalidBatchRequest;
    }
    const kind = keys.findComponentTerminator(scope, 1).? + 2;
    const head_key = try a.dupe(u8, scope);
    head_key[kind] = try scopes.physicalKind(scope, keys.producer_generation_head_kind);
    const raw_head = read.get(head_key) catch |err| if (err == error.NotFound) null else return err;
    const selected: ?publication.Digest = if (raw_head) |raw| blk: {
        const spec = try generations.Spec.decode(raw);
        if (!std.mem.eql(u8, &spec.scope_digest, &digest)) return error.ArtifactCatalogCorrupt;
        var selected_plan = try generations.Plan.init(a, scope, spec);
        defer selected_plan.deinit();
        const state = selected_plan.load(&read) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
        if (state.retiring or !std.meta.eql(state.progress, spec.output)) return error.ArtifactCatalogCorrupt;
        break :blk spec.id();
    } else null;
    const start = try a.alloc(u8, scope.len + 33);
    @memcpy(start[0..scope.len], scope);
    start[kind] = try scopes.physicalKind(scope, keys.producer_generation_state_kind);
    start[scope.len] = 1;
    @memcpy(start[scope.len + 1 ..], if (after) |cursor| &cursor.after else &@as(publication.Digest, @splat(0)));
    const prefix = start[0 .. scope.len + 1];
    var cursor = try read.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(if (after != null) start else prefix);
    if (after != null and entry != null and std.mem.eql(u8, entry.?.key, start)) entry = try cursor.next();
    var states: std.ArrayList(generations.State) = .empty;
    var bytes: usize = 0;
    var at_end = true;
    const started = time.monotonicNs();
    while (entry) |row| {
        if (!std.mem.startsWith(u8, row.key, prefix)) break;
        const size = row.key.len +| row.value.len;
        if (size > publication.max_payload_bytes) return error.ArtifactCatalogCorrupt;
        if (states.items.len != 0 and (states.items.len >= limits.rows or size > limits.bytes -| bytes or time.monotonicNs() -| started >= 2 * std.time.ns_per_ms)) {
            at_end = false;
            break;
        }
        if (row.key.len != prefix.len + 32) return error.ArtifactCatalogCorrupt;
        const state = try generations.State.decode(row.value);
        const id = state.spec.id();
        if (!std.mem.eql(u8, row.key[prefix.len..], &id) or !std.mem.eql(u8, &state.spec.scope_digest, &digest)) return error.ArtifactCatalogCorrupt;
        if (state.retiring and selected != null and std.mem.eql(u8, &selected.?, &id)) return error.ArtifactCatalogCorrupt;
        try states.append(a, state);
        bytes += size;
        entry = try cursor.next();
    }
    const next: ?Cursor = if (!at_end) .{ .authority = authority, .scope_digest = digest, .after = states.items[states.items.len - 1].spec.id() } else null;
    return .{ .arena = arena, .authority = authority, .selected = selected, .states = states.items, .next = next, .at_end = at_end };
}
