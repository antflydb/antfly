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

//! Durable dependency state for context-source enrichments.
//!
//! Authority vs. hints
//! -------------------
//! Everything authoritative is *document scoped* (keyed under the dependent
//! document prefix) so it moves with the row through splits, merges, restore,
//! and replication without bespoke handling:
//!
//! * `State` (one per dependent document and artifact): dependency
//!   generation, freshness, the registered subscription set, the last
//!   published input fingerprint/frontier, and bounded invalidation reasons.
//! * `Manifest` history (one per published artifact version): the immutable
//!   consumed-input lineage of that version.
//!
//! Two *global* families are verified hints that make the authoritative state
//! reachable by input identity without scanning dependents:
//!
//! * subscription hints `(spec, tuple) -> (artifact, doc)`, read when an input
//!   row changes (indexed invalidation);
//! * impact hints `(input table, input key, revision) -> (artifact, doc,
//!   version)`, read by reverse lineage queries.
//!
//! A hint is only acted on after the authoritative record confirms it (the
//! state still lists the tuple; the manifest version still exists and names
//! the input). Stale hints are therefore harmless and collected lazily, while
//! a *missing* hint would lose invalidation, so every authoritative write
//! writes its hints in the same atomic batch, and range moves copy hints for
//! the moved documents (`copyHintsForRange`).
//!
//! Lost-change protocol
//! --------------------
//! `planRegister` runs *before* context is read. It registers the bound
//! tuples (unioned with whatever is already registered) and captures the
//! dependency generation. Any input change committed after the read is
//! delivered after the registration is durable and therefore finds the
//! subscription and bumps the generation. `planPublish` fences on the
//! captured generation, so a computation that raced with an invalidation can
//! never overwrite a newer dependency generation; the invalidation has
//! already re-queued the document. Rebinding (a source row now references a
//! different organization) keeps the old tuples registered until the new
//! publication commits, then removes exactly the tuples no longer bound.
//!
//! All operations are *planners*: they read through a `Reader` and append the
//! required puts/deletes to a `Plan` that the caller commits atomically with
//! its own artifact writes under the runtime's write fence.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const ctx = @import("context_sources.zig");

pub const Digest = ctx.Digest;

/// Mirrors `internal_keys` layout. Document-scoped kinds follow the encoded
/// document key; global hint families live in the replay namespace under
/// `GlobalReplayKind.context_dependency`.
pub const user_namespace: u8 = 0x01;
pub const replay_namespace: u8 = 0x02;
pub const state_kind: u8 = 0x5c;
pub const history_kind: u8 = 0x5d;
pub const global_kind: u8 = 0x5e;
pub const subscription_family: u8 = 0x01;
pub const impact_family: u8 = 0x02;
pub const debounce_family: u8 = 0x03;

pub const max_reasons: usize = 8;
pub const default_retain_versions: u32 = 16;
pub const state_format: u8 = 1;
pub const manifest_format: u32 = 1;

pub const Error = error{
    DependencyGenerationSuperseded,
    InvalidDependencyState,
    OutOfMemory,
};

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

fn appendComponent(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    for (bytes) |b| {
        if (b == 0) {
            try list.appendSlice(alloc, &.{ 0, 0xff });
        } else try list.append(alloc, b);
    }
    try list.appendSlice(alloc, &.{ 0, 0 });
}

/// Decode one escaped component starting at `start`. Returns the decoded
/// bytes (owned) and the index just past the terminator.
fn decodeComponentAlloc(alloc: Allocator, key: []const u8, start: usize) !?struct { bytes: []u8, end: usize } {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var i = start;
    while (i + 1 < key.len) : (i += 1) {
        if (key[i] != 0) {
            try out.append(alloc, key[i]);
            continue;
        }
        if (key[i + 1] == 0) return .{ .bytes = try out.toOwnedSlice(alloc), .end = i + 2 };
        if (key[i + 1] != 0xff) break;
        try out.append(alloc, 0);
        i += 1;
    }
    out.deinit(alloc);
    return null;
}

fn appendDocPrefix(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, doc_key: []const u8) !void {
    try list.append(alloc, user_namespace);
    try appendComponent(list, alloc, doc_key);
}

pub fn stateKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try appendDocPrefix(&list, alloc, doc_key);
    try list.append(alloc, state_kind);
    try appendComponent(&list, alloc, artifact);
    return try list.toOwnedSlice(alloc);
}

pub fn historyPrefixAlloc(alloc: Allocator, doc_key: []const u8, artifact: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try appendDocPrefix(&list, alloc, doc_key);
    try list.append(alloc, history_kind);
    try appendComponent(&list, alloc, artifact);
    return try list.toOwnedSlice(alloc);
}

pub fn historyKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact: []const u8, version: u64) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try appendDocPrefix(&list, alloc, doc_key);
    try list.append(alloc, history_kind);
    try appendComponent(&list, alloc, artifact);
    var be: [8]u8 = undefined;
    std.mem.writeInt(u64, &be, version, .big);
    try list.appendSlice(alloc, &be);
    return try list.toOwnedSlice(alloc);
}

pub fn globalRootPrefix() [3]u8 {
    return .{ replay_namespace, 0xff, global_kind };
}

pub fn familyPrefix(family: u8) [4]u8 {
    return .{ replay_namespace, 0xff, global_kind, family };
}

pub fn subscriptionTuplePrefix(spec: Digest, tuple: Digest) [4 + 64]u8 {
    var out: [4 + 64]u8 = undefined;
    @memcpy(out[0..4], &familyPrefix(subscription_family));
    @memcpy(out[4..36], &spec);
    @memcpy(out[36..68], &tuple);
    return out;
}

pub fn subscriptionSpecPrefix(spec: Digest) [4 + 32]u8 {
    var out: [4 + 32]u8 = undefined;
    @memcpy(out[0..4], &familyPrefix(subscription_family));
    @memcpy(out[4..36], &spec);
    return out;
}

pub fn subscriptionKeyAlloc(alloc: Allocator, spec: Digest, tuple: Digest, artifact: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try list.appendSlice(alloc, &subscriptionTuplePrefix(spec, tuple));
    try appendComponent(&list, alloc, artifact);
    try appendComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub const SubscriptionHint = struct {
    spec: Digest,
    tuple: Digest,
    artifact: []u8,
    doc_key: []u8,

    pub fn deinit(self: *SubscriptionHint, alloc: Allocator) void {
        alloc.free(self.artifact);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub fn parseSubscriptionKeyAlloc(alloc: Allocator, key: []const u8) !?SubscriptionHint {
    if (key.len < 68 or !std.mem.eql(u8, key[0..4], &familyPrefix(subscription_family))) return null;
    const artifact = (try decodeComponentAlloc(alloc, key, 68)) orelse return null;
    errdefer alloc.free(artifact.bytes);
    const doc = (try decodeComponentAlloc(alloc, key, artifact.end)) orelse {
        alloc.free(artifact.bytes);
        return null;
    };
    if (doc.end != key.len) {
        alloc.free(artifact.bytes);
        alloc.free(doc.bytes);
        return null;
    }
    return .{ .spec = key[4..36].*, .tuple = key[36..68].*, .artifact = artifact.bytes, .doc_key = doc.bytes };
}

pub fn impactInputPrefixAlloc(alloc: Allocator, table_id: []const u8, input_key: []const u8, revision: ?Digest) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try list.appendSlice(alloc, &familyPrefix(impact_family));
    try appendComponent(&list, alloc, table_id);
    try appendComponent(&list, alloc, input_key);
    if (revision) |r| try list.appendSlice(alloc, &r);
    return try list.toOwnedSlice(alloc);
}

pub fn impactKeyAlloc(alloc: Allocator, table_id: []const u8, input_key: []const u8, revision: Digest, artifact: []const u8, doc_key: []const u8, version: u64) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try list.appendSlice(alloc, &familyPrefix(impact_family));
    try appendComponent(&list, alloc, table_id);
    try appendComponent(&list, alloc, input_key);
    try list.appendSlice(alloc, &revision);
    try appendComponent(&list, alloc, artifact);
    try appendComponent(&list, alloc, doc_key);
    var be: [8]u8 = undefined;
    std.mem.writeInt(u64, &be, version, .big);
    try list.appendSlice(alloc, &be);
    return try list.toOwnedSlice(alloc);
}

pub const ImpactHint = struct {
    table_id: []u8,
    input_key: []u8,
    revision: Digest,
    artifact: []u8,
    doc_key: []u8,
    version: u64,

    pub fn deinit(self: *ImpactHint, alloc: Allocator) void {
        alloc.free(self.table_id);
        alloc.free(self.input_key);
        alloc.free(self.artifact);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub fn parseImpactKeyAlloc(alloc: Allocator, key: []const u8) !?ImpactHint {
    if (key.len < 4 or !std.mem.eql(u8, key[0..4], &familyPrefix(impact_family))) return null;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const table = (try decodeComponentAlloc(a, key, 4)) orelse return null;
    const input = (try decodeComponentAlloc(a, key, table.end)) orelse return null;
    if (input.end + 32 > key.len) return null;
    const revision = key[input.end..][0..32].*;
    const artifact = (try decodeComponentAlloc(a, key, input.end + 32)) orelse return null;
    const doc = (try decodeComponentAlloc(a, key, artifact.end)) orelse return null;
    if (doc.end + 8 != key.len) return null;
    const table_id = try alloc.dupe(u8, table.bytes);
    errdefer alloc.free(table_id);
    const input_key = try alloc.dupe(u8, input.bytes);
    errdefer alloc.free(input_key);
    const artifact_name = try alloc.dupe(u8, artifact.bytes);
    errdefer alloc.free(artifact_name);
    return .{
        .table_id = table_id,
        .input_key = input_key,
        .revision = revision,
        .artifact = artifact_name,
        .doc_key = try alloc.dupe(u8, doc.bytes),
        .version = std.mem.readInt(u64, key[doc.end..][0..8], .big),
    };
}

/// Debounced refresh queue: `(due_ms, artifact, doc)`. Due-ordered so the
/// drain only ever reads the expired head.
pub fn debounceKeyAlloc(alloc: Allocator, due_ms: u64, artifact: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try list.appendSlice(alloc, &familyPrefix(debounce_family));
    var be: [8]u8 = undefined;
    std.mem.writeInt(u64, &be, due_ms, .big);
    try list.appendSlice(alloc, &be);
    try appendComponent(&list, alloc, artifact);
    try appendComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub const DebounceEntry = struct {
    due_ms: u64,
    artifact: []u8,
    doc_key: []u8,

    pub fn deinit(self: *DebounceEntry, alloc: Allocator) void {
        alloc.free(self.artifact);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub fn parseDebounceKeyAlloc(alloc: Allocator, key: []const u8) !?DebounceEntry {
    if (key.len < 12 or !std.mem.eql(u8, key[0..4], &familyPrefix(debounce_family))) return null;
    const artifact = (try decodeComponentAlloc(alloc, key, 12)) orelse return null;
    errdefer alloc.free(artifact.bytes);
    const doc = (try decodeComponentAlloc(alloc, key, artifact.end)) orelse {
        alloc.free(artifact.bytes);
        return null;
    };
    return .{ .due_ms = std.mem.readInt(u64, key[4..12], .big), .artifact = artifact.bytes, .doc_key = doc.bytes };
}

// ---------------------------------------------------------------------------
// Authoritative state record
// ---------------------------------------------------------------------------

pub const Freshness = enum(u8) {
    /// Registered but never successfully published.
    pending = 0,
    current = 1,
    stale = 2,
    /// The last rebuild failed; the last successful artifact (if any) is
    /// retained and still not current.
    failed = 3,
};

pub const Reason = struct {
    source: []const u8,
    change: ctx.ChangeKind,
    table_id: []const u8,
    key: []const u8,
};

pub const State = struct {
    /// Bumped by every invalidation; a publication must carry the generation
    /// it captured before reading context.
    generation: u64 = 1,
    /// Generation whose inputs the published artifact reflects (0 = none).
    published_generation: u64 = 0,
    /// Monotone version counter of published artifact manifests.
    published_version: u64 = 0,
    freshness: Freshness = .pending,
    /// A rebuild has been scheduled (journaled or debounced) and not yet run.
    rebuild_queued: bool = false,
    input_fingerprint: Digest = @splat(0),
    config_digest: Digest = @splat(0),
    /// Opaque per-source frontier (JSON) the published inputs were validated
    /// through.
    frontier: []const u8 = "",
    failure: []const u8 = "",
    /// Registered subscription set, sorted and unique.
    tuples: []const ctx.SubscriptionTuple = &.{},
    reasons: []const Reason = &.{},
    /// Reasons dropped beyond `max_reasons` since the last publication.
    dropped_reasons: u32 = 0,

    /// Owned decode result; free with `deinit`.
    pub fn deinit(self: *State, alloc: Allocator) void {
        alloc.free(self.frontier);
        alloc.free(self.failure);
        alloc.free(self.tuples);
        for (self.reasons) |r| {
            alloc.free(r.source);
            alloc.free(r.table_id);
            alloc.free(r.key);
        }
        alloc.free(self.reasons);
        self.* = undefined;
    }

    pub fn clone(self: State, alloc: Allocator) !State {
        var out = self;
        out.frontier = try alloc.dupe(u8, self.frontier);
        errdefer alloc.free(out.frontier);
        out.failure = try alloc.dupe(u8, self.failure);
        errdefer alloc.free(out.failure);
        out.tuples = try alloc.dupe(ctx.SubscriptionTuple, self.tuples);
        errdefer alloc.free(out.tuples);
        const reasons = try alloc.alloc(Reason, self.reasons.len);
        var n: usize = 0;
        errdefer {
            for (reasons[0..n]) |r| {
                alloc.free(r.source);
                alloc.free(r.table_id);
                alloc.free(r.key);
            }
            alloc.free(reasons);
        }
        for (self.reasons, 0..) |r, i| {
            const source = try alloc.dupe(u8, r.source);
            errdefer alloc.free(source);
            const table_id = try alloc.dupe(u8, r.table_id);
            errdefer alloc.free(table_id);
            reasons[i] = .{ .source = source, .change = r.change, .table_id = table_id, .key = try alloc.dupe(u8, r.key) };
            n += 1;
        }
        out.reasons = reasons;
        return out;
    }

    pub fn hasTuple(self: State, tuple: ctx.SubscriptionTuple) bool {
        var lo: usize = 0;
        var hi: usize = self.tuples.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.tuples[mid].eql(tuple)) return true;
            if (ctx.SubscriptionTuple.lessThan({}, self.tuples[mid], tuple)) lo = mid + 1 else hi = mid;
        }
        return false;
    }
};

fn writeU64(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, v: u64) !void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, v, .little);
    try out.appendSlice(alloc, &b);
}

fn writeBytes(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, @intCast(bytes.len), .little);
    try out.appendSlice(alloc, &b);
    try out.appendSlice(alloc, bytes);
}

pub fn encodeStateAlloc(alloc: Allocator, state: State) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, state_format);
    try writeU64(&out, alloc, state.generation);
    try writeU64(&out, alloc, state.published_generation);
    try writeU64(&out, alloc, state.published_version);
    try out.append(alloc, @backingInt(state.freshness));
    try out.append(alloc, @intFromBool(state.rebuild_queued));
    try out.appendSlice(alloc, &state.input_fingerprint);
    try out.appendSlice(alloc, &state.config_digest);
    try writeBytes(&out, alloc, state.frontier);
    try writeBytes(&out, alloc, state.failure);
    var n: [4]u8 = undefined;
    std.mem.writeInt(u32, &n, @intCast(state.tuples.len), .little);
    try out.appendSlice(alloc, &n);
    for (state.tuples) |t| {
        try out.appendSlice(alloc, &t.spec);
        try out.appendSlice(alloc, &t.tuple);
    }
    std.mem.writeInt(u32, &n, @intCast(state.reasons.len), .little);
    try out.appendSlice(alloc, &n);
    for (state.reasons) |r| {
        try writeBytes(&out, alloc, r.source);
        try out.append(alloc, @backingInt(r.change));
        try writeBytes(&out, alloc, r.table_id);
        try writeBytes(&out, alloc, r.key);
    }
    std.mem.writeInt(u32, &n, state.dropped_reasons, .little);
    try out.appendSlice(alloc, &n);
    return try out.toOwnedSlice(alloc);
}

const Cursor = struct {
    raw: []const u8,
    pos: usize = 0,

    fn take(self: *Cursor, n: usize) Error![]const u8 {
        if (self.pos + n > self.raw.len) return error.InvalidDependencyState;
        defer self.pos += n;
        return self.raw[self.pos..][0..n];
    }
    fn u8_(self: *Cursor) Error!u8 {
        return (try self.take(1))[0];
    }
    fn u32_(self: *Cursor) Error!u32 {
        return std.mem.readInt(u32, (try self.take(4))[0..4], .little);
    }
    fn u64_(self: *Cursor) Error!u64 {
        return std.mem.readInt(u64, (try self.take(8))[0..8], .little);
    }
    fn bytes(self: *Cursor) Error![]const u8 {
        return self.take(try self.u32_());
    }
};

pub fn decodeStateAlloc(alloc: Allocator, raw: []const u8) Error!State {
    var c = Cursor{ .raw = raw };
    if (try c.u8_() != state_format) return error.InvalidDependencyState;
    var view = State{};
    view.generation = try c.u64_();
    view.published_generation = try c.u64_();
    view.published_version = try c.u64_();
    view.freshness = std.enums.fromInt(Freshness, try c.u8_()) orelse return error.InvalidDependencyState;
    view.rebuild_queued = (try c.u8_()) != 0;
    view.input_fingerprint = (try c.take(32))[0..32].*;
    view.config_digest = (try c.take(32))[0..32].*;
    view.frontier = try c.bytes();
    view.failure = try c.bytes();
    const tuple_count = try c.u32_();
    if (tuple_count > (raw.len / 64) + 1) return error.InvalidDependencyState;
    const tuples = try alloc.alloc(ctx.SubscriptionTuple, tuple_count);
    defer alloc.free(tuples);
    for (tuples) |*t| t.* = .{ .spec = (try c.take(32))[0..32].*, .tuple = (try c.take(32))[0..32].* };
    view.tuples = tuples;
    const reason_count = try c.u32_();
    if (reason_count > max_reasons) return error.InvalidDependencyState;
    var reasons: [max_reasons]Reason = undefined;
    for (reasons[0..reason_count]) |*r| {
        const source = try c.bytes();
        const change = std.enums.fromInt(ctx.ChangeKind, try c.u8_()) orelse return error.InvalidDependencyState;
        r.* = .{ .source = source, .change = change, .table_id = try c.bytes(), .key = try c.bytes() };
    }
    view.reasons = reasons[0..reason_count];
    view.dropped_reasons = try c.u32_();
    if (c.pos != raw.len) return error.InvalidDependencyState;
    return view.clone(alloc);
}

// ---------------------------------------------------------------------------
// Versioned manifests (lineage)
// ---------------------------------------------------------------------------

pub const ManifestInput = struct {
    source: []const u8,
    table_id: []const u8,
    key: []const u8,
    revision: Digest,
};

pub const ManifestQuery = struct {
    source: []const u8,
    table_id: []const u8,
    query_identity: Digest,
    member_count: u32,
    /// Membership fingerprint over sorted (key, revision) pairs.
    membership: Digest,
    complete: bool = true,
};

/// Immutable lineage of one published artifact version: the inputs actually
/// consumed (historical provenance) — never inputs that only became
/// applicable later, which live subscriptions cover.
pub const Manifest = struct {
    version: u64,
    generation: u64,
    input_fingerprint: Digest,
    config_digest: Digest,
    producer_recipe: []const u8 = "",
    frontier: []const u8 = "",
    published_at_ms: i64 = 0,
    inputs: []const ManifestInput = &.{},
    queries: []const ManifestQuery = &.{},
    /// Missing document references observed at capture (optional absent).
    absent: []const ManifestInput = &.{},
};

pub fn encodeManifestAlloc(alloc: Allocator, m: Manifest) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.print(alloc, "{{\"format\":{d},\"version\":{d},\"generation\":{d},\"input_fingerprint\":\"sha256:{x}\",\"config_digest\":\"sha256:{x}\",\"published_at_ms\":{d},\"producer_recipe\":", .{
        manifest_format, m.version, m.generation, &m.input_fingerprint, &m.config_digest, m.published_at_ms,
    });
    try ctx.appendJsonString(alloc, &out, m.producer_recipe);
    try out.appendSlice(alloc, ",\"frontier\":");
    try ctx.appendJsonString(alloc, &out, m.frontier);
    inline for (.{ "inputs", "absent" }) |name| {
        try out.appendSlice(alloc, ",\"" ++ name ++ "\":[");
        for (@field(m, name), 0..) |input, i| {
            if (i != 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"source\":");
            try ctx.appendJsonString(alloc, &out, input.source);
            try out.appendSlice(alloc, ",\"table_id\":");
            try ctx.appendJsonString(alloc, &out, input.table_id);
            try out.appendSlice(alloc, ",\"key\":");
            try ctx.appendJsonString(alloc, &out, input.key);
            try out.print(alloc, ",\"revision\":\"sha256:{x}\"}}", .{&input.revision});
        }
        try out.append(alloc, ']');
    }
    try out.appendSlice(alloc, ",\"queries\":[");
    for (m.queries, 0..) |q, i| {
        if (i != 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "{\"source\":");
        try ctx.appendJsonString(alloc, &out, q.source);
        try out.appendSlice(alloc, ",\"table_id\":");
        try ctx.appendJsonString(alloc, &out, q.table_id);
        try out.print(alloc, ",\"query_identity\":\"sha256:{x}\",\"member_count\":{d},\"membership\":\"sha256:{x}\",\"complete\":{}}}", .{
            &q.query_identity, q.member_count, &q.membership, q.complete,
        });
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub const DecodedManifest = struct {
    parsed: std.json.Parsed(std.json.Value),
    version: u64,
    generation: u64,

    pub fn deinit(self: *DecodedManifest) void {
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn value(self: *const DecodedManifest) std.json.Value {
        return self.parsed.value;
    }

    /// Whether this manifest consumed `(table_id, key[, revision])`.
    pub fn consumed(self: *const DecodedManifest, table_id: []const u8, key: []const u8, revision: ?Digest) bool {
        const inputs = self.parsed.value.object.get("inputs") orelse return false;
        if (inputs != .array) return false;
        var hex_buf: [7 + 64]u8 = undefined;
        const want_revision: ?[]const u8 = if (revision) |r| std.fmt.bufPrint(&hex_buf, "sha256:{x}", .{&r}) catch unreachable else null;
        for (inputs.array.items) |input| {
            if (input != .object) continue;
            const t = input.object.get("table_id") orelse continue;
            const k = input.object.get("key") orelse continue;
            if (t != .string or k != .string) continue;
            if (!std.mem.eql(u8, t.string, table_id) or !std.mem.eql(u8, k.string, key)) continue;
            if (want_revision) |w| {
                const rv = input.object.get("revision") orelse continue;
                if (rv != .string or !std.mem.eql(u8, rv.string, w)) continue;
            }
            return true;
        }
        return false;
    }
};

pub fn decodeManifestAlloc(alloc: Allocator, raw: []const u8) !DecodedManifest {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidDependencyState,
    };
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidDependencyState;
    const version = parsed.value.object.get("version") orelse return error.InvalidDependencyState;
    const generation = parsed.value.object.get("generation") orelse return error.InvalidDependencyState;
    if (version != .integer or generation != .integer) return error.InvalidDependencyState;
    return .{ .parsed = parsed, .version = @intCast(version.integer), .generation = @intCast(generation.integer) };
}

pub fn membershipDigest(members: []const ctx.ConsumedInput) Digest {
    var h = Sha256.init(.{});
    for (members) |m| {
        h.update(m.key);
        h.update(&.{0});
        h.update(&m.revision());
    }
    var out: Digest = undefined;
    h.final(&out);
    return out;
}

// ---------------------------------------------------------------------------
// Reader / Plan
// ---------------------------------------------------------------------------

pub const Entry = struct { key: []const u8, value: []const u8 };

/// Read access the planners need. `scan` visits keys in `[lower, upper)` in
/// order; the visitor returns false to stop. Values are borrowed for the
/// duration of the callback.
pub const Reader = struct {
    ptr: *anyopaque,
    get_fn: *const fn (ptr: *anyopaque, alloc: Allocator, key: []const u8) anyerror!?[]u8,
    scan_fn: *const fn (ptr: *anyopaque, lower: []const u8, upper: []const u8, visitor: Visitor) anyerror!void,

    pub const Visitor = struct {
        ptr: *anyopaque,
        visit_fn: *const fn (ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!bool,
    };

    pub fn get(self: Reader, alloc: Allocator, key: []const u8) !?[]u8 {
        return self.get_fn(self.ptr, alloc, key);
    }

    pub fn scan(self: Reader, lower: []const u8, upper: []const u8, visitor: Visitor) !void {
        return self.scan_fn(self.ptr, lower, upper, visitor);
    }

    pub fn scanPrefix(self: Reader, alloc: Allocator, prefix: []const u8, visitor: Visitor) !void {
        const upper = try prefixUpperAlloc(alloc, prefix);
        defer alloc.free(upper);
        try self.scan(prefix, upper, visitor);
    }
};

pub fn prefixUpperAlloc(alloc: Allocator, prefix: []const u8) ![]u8 {
    var upper = try alloc.dupe(u8, prefix);
    var i = upper.len;
    while (i > 0) {
        i -= 1;
        if (upper[i] != 0xff) {
            upper[i] += 1;
            return alloc.realloc(upper, i + 1);
        }
    }
    alloc.free(upper);
    return alloc.dupe(u8, "\xff\xff\xff\xff\xff\xff\xff\xff");
}

/// Writes for the caller to commit atomically. Later operations on the same
/// key win, so planners may be composed within one plan.
pub const Plan = struct {
    alloc: Allocator,
    puts: std.ArrayListUnmanaged(Entry) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,

    pub fn init(alloc: Allocator) Plan {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Plan) void {
        for (self.puts.items) |e| {
            self.alloc.free(e.key);
            self.alloc.free(e.value);
        }
        self.puts.deinit(self.alloc);
        for (self.deletes.items) |k| self.alloc.free(k);
        self.deletes.deinit(self.alloc);
        self.* = undefined;
    }

    /// Takes ownership of key and value.
    pub fn putOwned(self: *Plan, key: []u8, value: []u8) !void {
        self.dropDelete(key);
        for (self.puts.items) |*e| if (std.mem.eql(u8, e.key, key)) {
            self.alloc.free(e.value);
            self.alloc.free(key);
            e.value = value;
            return;
        };
        try self.puts.append(self.alloc, .{ .key = key, .value = value });
    }

    pub fn put(self: *Plan, key: []const u8, value: []const u8) !void {
        const k = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(k);
        try self.putOwned(k, try self.alloc.dupe(u8, value));
    }

    pub fn deleteOwned(self: *Plan, key: []u8) !void {
        var i: usize = 0;
        while (i < self.puts.items.len) {
            if (std.mem.eql(u8, self.puts.items[i].key, key)) {
                const e = self.puts.swapRemove(i);
                self.alloc.free(e.key);
                self.alloc.free(e.value);
            } else i += 1;
        }
        for (self.deletes.items) |k| if (std.mem.eql(u8, k, key)) {
            self.alloc.free(key);
            return;
        };
        try self.deletes.append(self.alloc, key);
    }

    pub fn delete(self: *Plan, key: []const u8) !void {
        try self.deleteOwned(try self.alloc.dupe(u8, key));
    }

    fn dropDelete(self: *Plan, key: []const u8) void {
        var i: usize = 0;
        while (i < self.deletes.items.len) {
            if (std.mem.eql(u8, self.deletes.items[i], key)) {
                self.alloc.free(self.deletes.swapRemove(i));
            } else i += 1;
        }
    }

    /// Pending value of `key` within this plan: `.put` bytes, `.deleted`, or
    /// `.untouched` (read through to storage).
    pub fn pending(self: *const Plan, key: []const u8) union(enum) { put: []const u8, deleted, untouched } {
        for (self.puts.items) |e| if (std.mem.eql(u8, e.key, key)) return .{ .put = e.value };
        for (self.deletes.items) |k| if (std.mem.eql(u8, k, key)) return .deleted;
        return .untouched;
    }
};

pub fn loadState(alloc: Allocator, reader: Reader, plan: ?*const Plan, doc_key: []const u8, artifact: []const u8) !?State {
    const key = try stateKeyAlloc(alloc, doc_key, artifact);
    defer alloc.free(key);
    if (plan) |p| switch (p.pending(key)) {
        .put => |raw| return try decodeStateAlloc(alloc, raw),
        .deleted => return null,
        .untouched => {},
    };
    const raw = (try reader.get(alloc, key)) orelse return null;
    defer alloc.free(raw);
    return try decodeStateAlloc(alloc, raw);
}

fn storeState(plan: *Plan, doc_key: []const u8, artifact: []const u8, state: State) !void {
    const key = try stateKeyAlloc(plan.alloc, doc_key, artifact);
    errdefer plan.alloc.free(key);
    try plan.putOwned(key, try encodeStateAlloc(plan.alloc, state));
}

// ---------------------------------------------------------------------------
// Planners
// ---------------------------------------------------------------------------

pub const Capture = struct {
    generation: u64,
    config_digest: Digest,
};

/// Register `tuples` for `(doc, artifact)` before its context is read and
/// capture the dependency generation used to fence publication. Tuples
/// already registered (including a previous binding's) remain registered.
/// A configuration change (different digest) is itself an invalidation: the
/// generation advances so an in-flight computation under the old declaration
/// cannot publish.
pub fn planRegister(
    alloc: Allocator,
    reader: Reader,
    plan: *Plan,
    doc_key: []const u8,
    artifact: []const u8,
    tuples: []const ctx.SubscriptionTuple,
    config_digest: Digest,
) !Capture {
    const existing = try loadState(alloc, reader, plan, doc_key, artifact);
    const loaded = existing != null;
    // Zero-length default slices make `deinit` safe for a fresh state too.
    var state = existing orelse State{};
    defer state.deinit(alloc);
    var union_list = std.ArrayListUnmanaged(ctx.SubscriptionTuple).empty;
    defer union_list.deinit(alloc);
    try union_list.appendSlice(alloc, state.tuples);
    var changed = false;
    for (tuples) |t| {
        if (state.hasTuple(t)) continue;
        try union_list.append(alloc, t);
        const key = try subscriptionKeyAlloc(alloc, t.spec, t.tuple, artifact, doc_key);
        errdefer alloc.free(key);
        try plan.putOwned(key, try alloc.dupe(u8, ""));
        changed = true;
    }
    ctx.sortUniqueTuples(&union_list);
    const config_changed = !std.mem.eql(u8, &state.config_digest, &config_digest);
    if (config_changed and state.published_generation != 0) {
        state.generation += 1;
        if (state.freshness == .current) state.freshness = .stale;
    }
    state.config_digest = config_digest;
    if (changed or config_changed or !loaded) {
        const previous_tuples = state.tuples;
        state.tuples = union_list.items;
        defer state.tuples = previous_tuples;
        try storeState(plan, doc_key, artifact, state);
    }
    return .{ .generation = state.generation, .config_digest = config_digest };
}

pub const PublishInput = struct {
    capture: Capture,
    /// Exact tuples bound by the published computation.
    tuples: []const ctx.SubscriptionTuple,
    input_fingerprint: Digest,
    frontier: []const u8 = "",
    producer_recipe: []const u8 = "",
    published_at_ms: i64 = 0,
    inputs: []const ManifestInput = &.{},
    queries: []const ManifestQuery = &.{},
    absent: []const ManifestInput = &.{},
    retain_versions: u32 = default_retain_versions,
    /// When false the artifact bytes were reused (skip state matched); only
    /// freshness advances and no new lineage version is minted.
    new_version: bool = true,
};

pub const PublishResult = struct {
    version: u64,
};

/// Fence on the captured generation, mark the artifact current, replace the
/// subscription set with exactly the tuples now bound, and (for a new output
/// version) append an immutable manifest plus its impact hints while pruning
/// versions beyond retention.
pub fn planPublish(alloc: Allocator, reader: Reader, plan: *Plan, doc_key: []const u8, artifact: []const u8, input: PublishInput) !PublishResult {
    var state = (try loadState(alloc, reader, plan, doc_key, artifact)) orelse return error.DependencyGenerationSuperseded;
    defer state.deinit(alloc);
    if (state.generation != input.capture.generation or
        !std.mem.eql(u8, &state.config_digest, &input.capture.config_digest))
        return error.DependencyGenerationSuperseded;

    // Remove subscriptions no longer bound.
    for (state.tuples) |old| {
        var still = false;
        for (input.tuples) |t| if (t.eql(old)) {
            still = true;
            break;
        };
        if (still) continue;
        try plan.deleteOwned(try subscriptionKeyAlloc(alloc, old.spec, old.tuple, artifact, doc_key));
    }
    var sorted = std.ArrayListUnmanaged(ctx.SubscriptionTuple).empty;
    defer sorted.deinit(alloc);
    try sorted.appendSlice(alloc, input.tuples);
    ctx.sortUniqueTuples(&sorted);
    for (sorted.items) |t| if (!state.hasTuple(t)) {
        const key = try subscriptionKeyAlloc(alloc, t.spec, t.tuple, artifact, doc_key);
        errdefer alloc.free(key);
        try plan.putOwned(key, try alloc.dupe(u8, ""));
    };

    var version = state.published_version;
    if (input.new_version or version == 0) {
        version += 1;
        const manifest = try encodeManifestAlloc(alloc, .{
            .version = version,
            .generation = state.generation,
            .input_fingerprint = input.input_fingerprint,
            .config_digest = input.capture.config_digest,
            .producer_recipe = input.producer_recipe,
            .frontier = input.frontier,
            .published_at_ms = input.published_at_ms,
            .inputs = input.inputs,
            .queries = input.queries,
            .absent = input.absent,
        });
        errdefer alloc.free(manifest);
        const hkey = try historyKeyAlloc(alloc, doc_key, artifact, version);
        errdefer alloc.free(hkey);
        try plan.putOwned(hkey, manifest);
        for (input.inputs) |in| {
            const ikey = try impactKeyAlloc(alloc, in.table_id, in.key, in.revision, artifact, doc_key, version);
            errdefer alloc.free(ikey);
            try plan.putOwned(ikey, try alloc.dupe(u8, ""));
        }
        const retain = @max(input.retain_versions, 1);
        if (version > retain) try planDropVersion(alloc, reader, plan, doc_key, artifact, version - retain);
    }

    const next = State{
        .generation = state.generation,
        .published_generation = state.generation,
        .published_version = version,
        .freshness = .current,
        .rebuild_queued = false,
        .input_fingerprint = input.input_fingerprint,
        .config_digest = input.capture.config_digest,
        .frontier = input.frontier,
        .tuples = sorted.items,
    };
    try storeState(plan, doc_key, artifact, next);
    return .{ .version = version };
}

/// Record a failed (or budget-refused) rebuild. The last successful artifact
/// stays retained but is reported as failed rather than current; the
/// subscriptions registered for the attempt remain so a later input change
/// retries it.
pub fn planFailure(alloc: Allocator, reader: Reader, plan: *Plan, doc_key: []const u8, artifact: []const u8, capture: Capture, failure: []const u8) !void {
    var state = (try loadState(alloc, reader, plan, doc_key, artifact)) orelse return;
    defer state.deinit(alloc);
    if (state.generation != capture.generation) return error.DependencyGenerationSuperseded;
    const previous = state.failure;
    defer state.failure = previous;
    state.failure = failure[0..@min(failure.len, 512)];
    const previous_freshness = state.freshness;
    defer state.freshness = previous_freshness;
    state.freshness = .failed;
    const previous_queued = state.rebuild_queued;
    defer state.rebuild_queued = previous_queued;
    state.rebuild_queued = false;
    try storeState(plan, doc_key, artifact, state);
}

fn planDropVersion(alloc: Allocator, reader: Reader, plan: *Plan, doc_key: []const u8, artifact: []const u8, version: u64) !void {
    const hkey = try historyKeyAlloc(alloc, doc_key, artifact, version);
    defer alloc.free(hkey);
    const raw = (try reader.get(alloc, hkey)) orelse return;
    defer alloc.free(raw);
    var manifest = decodeManifestAlloc(alloc, raw) catch {
        try plan.delete(hkey);
        return;
    };
    defer manifest.deinit();
    if (manifest.value().object.get("inputs")) |inputs| if (inputs == .array) for (inputs.array.items) |item| {
        if (item != .object) continue;
        const t = item.object.get("table_id") orelse continue;
        const k = item.object.get("key") orelse continue;
        const r = item.object.get("revision") orelse continue;
        if (t != .string or k != .string or r != .string) continue;
        const revision = parseRevision(r.string) orelse continue;
        try plan.deleteOwned(try impactKeyAlloc(alloc, t.string, k.string, revision, artifact, doc_key, version));
    };
    try plan.delete(hkey);
}

pub fn parseRevision(text: []const u8) ?Digest {
    const hex = if (std.mem.startsWith(u8, text, "sha256:")) text[7..] else text;
    if (hex.len != 64) return null;
    var out: Digest = undefined;
    _ = std.fmt.hexToBytes(&out, hex) catch return null;
    return out;
}

pub const InvalidationTarget = struct {
    artifact: []const u8,
    doc_key: []const u8,
};

pub const Invalidated = struct {
    artifact: []u8,
    doc_key: []u8,
    /// Freshly marked stale by this change (false when already stale).
    newly_stale: bool,
};

pub const InvalidateOptions = struct {
    source: []const u8,
    change: ctx.ChangeKind,
    table_id: []const u8,
    key: []const u8,
    /// Manual refresh only marks stale; background also queues a rebuild.
    queue_rebuild: bool = true,
    /// Restrict to documents inside this shard's range (lower inclusive,
    /// upper exclusive, empty = unbounded) so foreign hints left behind by a
    /// split are ignored.
    range_lower: []const u8 = "",
    range_upper: []const u8 = "",
    /// Resume after this subscription key (exclusive). Empty starts at the
    /// tuple prefix.
    resume_after: []const u8 = "",
    max_targets: usize = 1024,
};

pub const InvalidatePage = struct {
    targets: std.ArrayListUnmanaged(Invalidated) = .empty,
    /// Non-null when the page budget stopped the scan; pass as `resume_after`.
    resume_after: ?[]u8 = null,

    pub fn deinit(self: *InvalidatePage, alloc: Allocator) void {
        for (self.targets.items) |t| {
            alloc.free(t.artifact);
            alloc.free(t.doc_key);
        }
        self.targets.deinit(alloc);
        if (self.resume_after) |r| alloc.free(r);
        self.* = undefined;
    }
};

/// Invalidate every dependent subscribed to `(spec, tuple)`, bounded by
/// `max_targets` per page. Each confirmed dependent's generation advances,
/// its freshness becomes stale with a bounded reason, and stale hints (the
/// authoritative state no longer lists the tuple) are deleted.
pub fn planInvalidateTuple(
    alloc: Allocator,
    reader: Reader,
    plan: *Plan,
    spec: Digest,
    tuple: Digest,
    options: InvalidateOptions,
) !InvalidatePage {
    const prefix = subscriptionTuplePrefix(spec, tuple);
    return planInvalidateRange(alloc, reader, plan, &prefix, spec, tuple, options);
}

/// Spec-wide invalidation for a row whose tuple expansion overflowed.
pub fn planInvalidateSpec(alloc: Allocator, reader: Reader, plan: *Plan, spec: Digest, options: InvalidateOptions) !InvalidatePage {
    const prefix = subscriptionSpecPrefix(spec);
    return planInvalidateRange(alloc, reader, plan, &prefix, spec, null, options);
}

fn planInvalidateRange(
    alloc: Allocator,
    reader: Reader,
    plan: *Plan,
    prefix: []const u8,
    spec: Digest,
    tuple: ?Digest,
    options: InvalidateOptions,
) !InvalidatePage {
    _ = spec;
    _ = tuple;
    var page = InvalidatePage{};
    errdefer page.deinit(alloc);
    const upper = try prefixUpperAlloc(alloc, prefix);
    defer alloc.free(upper);
    const lower = if (options.resume_after.len != 0) try std.mem.concat(alloc, u8, &.{ options.resume_after, "\x00" }) else try alloc.dupe(u8, prefix);
    defer alloc.free(lower);

    var hints = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (hints.items) |h| alloc.free(h);
        hints.deinit(alloc);
    }
    const Collect = struct {
        alloc: Allocator,
        hints: *std.ArrayListUnmanaged([]u8),
        limit: usize,
        fn visit(p: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(p));
            try self.hints.append(self.alloc, try self.alloc.dupe(u8, key));
            return self.hints.items.len <= self.limit;
        }
    };
    var collect = Collect{ .alloc = alloc, .hints = &hints, .limit = options.max_targets };
    try reader.scan(lower, upper, .{ .ptr = &collect, .visit_fn = Collect.visit });
    const more = hints.items.len > options.max_targets;
    const visible = if (more) hints.items[0..options.max_targets] else hints.items;

    for (visible) |hint_key| {
        var hint = (try parseSubscriptionKeyAlloc(alloc, hint_key)) orelse {
            try plan.delete(hint_key);
            continue;
        };
        defer hint.deinit(alloc);
        if (!inRange(hint.doc_key, options.range_lower, options.range_upper)) continue;
        var state = (try loadState(alloc, reader, plan, hint.doc_key, hint.artifact)) orelse {
            try plan.delete(hint_key);
            continue;
        };
        defer state.deinit(alloc);
        if (!state.hasTuple(.{ .spec = hint.spec, .tuple = hint.tuple })) {
            try plan.delete(hint_key);
            continue;
        }
        const newly_stale = try applyInvalidation(alloc, plan, hint.doc_key, hint.artifact, &state, options);
        const artifact = try alloc.dupe(u8, hint.artifact);
        errdefer alloc.free(artifact);
        const doc_key = try alloc.dupe(u8, hint.doc_key);
        errdefer alloc.free(doc_key);
        try page.targets.append(alloc, .{ .artifact = artifact, .doc_key = doc_key, .newly_stale = newly_stale });
    }
    if (more) page.resume_after = try alloc.dupe(u8, visible[visible.len - 1]);
    return page;
}

/// Invalidate one known dependent directly (upstream artifact refresh,
/// administrative refresh, or access revocation).
pub fn planInvalidateDocument(alloc: Allocator, reader: Reader, plan: *Plan, doc_key: []const u8, artifact: []const u8, options: InvalidateOptions) !bool {
    var state = (try loadState(alloc, reader, plan, doc_key, artifact)) orelse return false;
    defer state.deinit(alloc);
    _ = try applyInvalidation(alloc, plan, doc_key, artifact, &state, options);
    return true;
}

fn applyInvalidation(alloc: Allocator, plan: *Plan, doc_key: []const u8, artifact: []const u8, state: *State, options: InvalidateOptions) !bool {
    const newly_stale = state.freshness == .current or state.freshness == .pending;
    var reasons = std.ArrayListUnmanaged(Reason).empty;
    defer reasons.deinit(alloc);
    try reasons.appendSlice(alloc, state.reasons);
    var dropped = state.dropped_reasons;
    var duplicate = false;
    for (state.reasons) |r| {
        if (std.mem.eql(u8, r.source, options.source) and r.change == options.change and
            std.mem.eql(u8, r.table_id, options.table_id) and std.mem.eql(u8, r.key, options.key)) duplicate = true;
    }
    if (!duplicate) {
        if (reasons.items.len < max_reasons) {
            try reasons.append(alloc, .{ .source = options.source, .change = options.change, .table_id = options.table_id, .key = options.key });
        } else dropped +|= 1;
    }
    var next = state.*;
    next.generation = state.generation + 1;
    next.freshness = if (state.freshness == .pending) .pending else .stale;
    next.rebuild_queued = state.rebuild_queued or options.queue_rebuild;
    next.reasons = reasons.items;
    next.dropped_reasons = dropped;
    try storeState(plan, doc_key, artifact, next);
    return newly_stale;
}

/// Remove all live dependency state for `(doc, artifact)`: the state record,
/// its subscription hints, and (when `erase_history`) every retained manifest
/// and impact hint. Used when the dependent row or the enrichment is deleted.
pub fn planCleanup(alloc: Allocator, reader: Reader, plan: *Plan, doc_key: []const u8, artifact: []const u8, erase_history: bool) !void {
    if (try loadState(alloc, reader, plan, doc_key, artifact)) |loaded| {
        var state = loaded;
        defer state.deinit(alloc);
        for (state.tuples) |t| try plan.deleteOwned(try subscriptionKeyAlloc(alloc, t.spec, t.tuple, artifact, doc_key));
        if (erase_history) {
            var v: u64 = state.published_version;
            const floor = if (v > default_retain_versions * 4) v - default_retain_versions * 4 else 0;
            while (v > floor) : (v -= 1) try planDropVersion(alloc, reader, plan, doc_key, artifact, v);
        }
    }
    try plan.deleteOwned(try stateKeyAlloc(alloc, doc_key, artifact));
}

pub fn inRange(doc_key: []const u8, lower: []const u8, upper: []const u8) bool {
    if (lower.len != 0 and std.mem.lessThan(u8, doc_key, lower)) return false;
    if (upper.len != 0 and !std.mem.lessThan(u8, doc_key, upper)) return false;
    return true;
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

pub const ImpactRow = struct {
    artifact: []u8,
    doc_key: []u8,
    version: u64,
    revision: Digest,
    /// The version is the artifact's latest publication.
    current_version: bool,
    freshness: Freshness,

    pub fn deinit(self: *ImpactRow, alloc: Allocator) void {
        alloc.free(self.artifact);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub const ImpactPage = struct {
    rows: std.ArrayListUnmanaged(ImpactRow) = .empty,
    resume_after: ?[]u8 = null,
    /// Hint rows examined (budget accounting, includes skipped stale hints).
    examined: usize = 0,

    pub fn deinit(self: *ImpactPage, alloc: Allocator) void {
        for (self.rows.items) |*r| r.deinit(alloc);
        self.rows.deinit(alloc);
        if (self.resume_after) |r| alloc.free(r);
        self.* = undefined;
    }
};

/// Direct reverse lineage: retained artifact versions whose manifests
/// consumed `(table_id, key[, revision])`. Hints are confirmed against the
/// manifest so retention cleanup racing with a lookup can only omit a
/// version that no longer exists, never fabricate one.
pub fn queryImpact(
    alloc: Allocator,
    reader: Reader,
    table_id: []const u8,
    input_key: []const u8,
    revision: ?Digest,
    resume_after: []const u8,
    limit: usize,
    range_lower: []const u8,
    range_upper: []const u8,
) !ImpactPage {
    var page = ImpactPage{};
    errdefer page.deinit(alloc);
    const prefix = try impactInputPrefixAlloc(alloc, table_id, input_key, revision);
    defer alloc.free(prefix);
    const upper = try prefixUpperAlloc(alloc, prefix);
    defer alloc.free(upper);
    const lower = if (resume_after.len != 0) try std.mem.concat(alloc, u8, &.{ resume_after, "\x00" }) else try alloc.dupe(u8, prefix);
    defer alloc.free(lower);
    var keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (keys.items) |k| alloc.free(k);
        keys.deinit(alloc);
    }
    const Collect = struct {
        alloc: Allocator,
        keys: *std.ArrayListUnmanaged([]u8),
        limit: usize,
        fn visit(p: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(p));
            try self.keys.append(self.alloc, try self.alloc.dupe(u8, key));
            return self.keys.items.len <= self.limit;
        }
    };
    var collect = Collect{ .alloc = alloc, .keys = &keys, .limit = limit };
    try reader.scan(lower, upper, .{ .ptr = &collect, .visit_fn = Collect.visit });
    const more = keys.items.len > limit;
    const visible = if (more) keys.items[0..limit] else keys.items;
    for (visible) |key| {
        page.examined += 1;
        var hint = (try parseImpactKeyAlloc(alloc, key)) orelse continue;
        defer hint.deinit(alloc);
        if (!inRange(hint.doc_key, range_lower, range_upper)) continue;
        const hkey = try historyKeyAlloc(alloc, hint.doc_key, hint.artifact, hint.version);
        defer alloc.free(hkey);
        const raw = (try reader.get(alloc, hkey)) orelse continue;
        defer alloc.free(raw);
        var manifest = decodeManifestAlloc(alloc, raw) catch continue;
        defer manifest.deinit();
        if (!manifest.consumed(table_id, input_key, hint.revision)) continue;
        var state = try loadState(alloc, reader, null, hint.doc_key, hint.artifact);
        defer if (state) |*s| s.deinit(alloc);
        const artifact = try alloc.dupe(u8, hint.artifact);
        errdefer alloc.free(artifact);
        const doc_key = try alloc.dupe(u8, hint.doc_key);
        errdefer alloc.free(doc_key);
        try page.rows.append(alloc, .{
            .artifact = artifact,
            .doc_key = doc_key,
            .version = hint.version,
            .revision = hint.revision,
            .current_version = if (state) |s| s.published_version == hint.version else false,
            .freshness = if (state) |s| s.freshness else .stale,
        });
    }
    if (more) page.resume_after = try alloc.dupe(u8, visible[visible.len - 1]);
    return page;
}

pub const LiveDependent = struct {
    artifact: []u8,
    doc_key: []u8,
};

/// Live dependents of a subscription tuple, confirmed against authoritative
/// state. These are rebuild *candidates*, distinct from historical lineage.
pub fn liveDependentsAlloc(alloc: Allocator, reader: Reader, spec: Digest, tuple: Digest, limit: usize) ![]LiveDependent {
    var out = std.ArrayListUnmanaged(LiveDependent).empty;
    errdefer {
        for (out.items) |d| {
            alloc.free(d.artifact);
            alloc.free(d.doc_key);
        }
        out.deinit(alloc);
    }
    const prefix = subscriptionTuplePrefix(spec, tuple);
    var keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (keys.items) |k| alloc.free(k);
        keys.deinit(alloc);
    }
    const Collect = struct {
        alloc: Allocator,
        keys: *std.ArrayListUnmanaged([]u8),
        limit: usize,
        fn visit(p: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(p));
            try self.keys.append(self.alloc, try self.alloc.dupe(u8, key));
            return self.keys.items.len < self.limit;
        }
    };
    var collect = Collect{ .alloc = alloc, .keys = &keys, .limit = limit };
    try reader.scanPrefix(alloc, &prefix, .{ .ptr = &collect, .visit_fn = Collect.visit });
    for (keys.items) |key| {
        var hint = (try parseSubscriptionKeyAlloc(alloc, key)) orelse continue;
        var keep = false;
        defer if (!keep) hint.deinit(alloc);
        var state = (try loadState(alloc, reader, null, hint.doc_key, hint.artifact)) orelse continue;
        defer state.deinit(alloc);
        if (!state.hasTuple(.{ .spec = hint.spec, .tuple = hint.tuple })) continue;
        try out.append(alloc, .{ .artifact = hint.artifact, .doc_key = hint.doc_key });
        keep = true;
    }
    return try out.toOwnedSlice(alloc);
}

/// Copy subscription and impact hints for documents in `[lower, upper)` from
/// `source` into `plan` (for a split destination or restore target). Hints
/// are a pure function of document-scoped state, so copying the subset of
/// hints is equivalent to rebuilding them and never scans document rows.
pub fn copyHintsForRange(alloc: Allocator, source: Reader, plan: *Plan, lower: []const u8, upper: []const u8) !usize {
    const Copy = struct {
        alloc: Allocator,
        plan: *Plan,
        lower: []const u8,
        upper: []const u8,
        copied: usize = 0,
        fn visit(p: *anyopaque, key: []const u8, value: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(p));
            const doc_key: []u8 = blk: {
                if (key.len >= 4 and key[3] == subscription_family) {
                    const hint = (try parseSubscriptionKeyAlloc(self.alloc, key)) orelse return true;
                    self.alloc.free(hint.artifact);
                    break :blk hint.doc_key;
                } else if (key.len >= 4 and key[3] == impact_family) {
                    const hint = (try parseImpactKeyAlloc(self.alloc, key)) orelse return true;
                    self.alloc.free(hint.table_id);
                    self.alloc.free(hint.input_key);
                    self.alloc.free(hint.artifact);
                    break :blk hint.doc_key;
                } else if (key.len >= 4 and key[3] == debounce_family) {
                    const entry = (try parseDebounceKeyAlloc(self.alloc, key)) orelse return true;
                    self.alloc.free(entry.artifact);
                    break :blk entry.doc_key;
                } else return true;
            };
            defer self.alloc.free(doc_key);
            if (!inRange(doc_key, self.lower, self.upper)) return true;
            try self.plan.put(key, value);
            self.copied += 1;
            return true;
        }
    };
    var copy = Copy{ .alloc = alloc, .plan = plan, .lower = lower, .upper = upper };
    try source.scanPrefix(alloc, &globalRootPrefix(), .{ .ptr = &copy, .visit_fn = Copy.visit });
    return copy.copied;
}

// ---------------------------------------------------------------------------
// Debounced refresh queue
// ---------------------------------------------------------------------------

pub fn planQueueDebounced(plan: *Plan, due_ms: u64, artifact: []const u8, doc_key: []const u8) !void {
    const key = try debounceKeyAlloc(plan.alloc, due_ms, artifact, doc_key);
    errdefer plan.alloc.free(key);
    try plan.putOwned(key, try plan.alloc.dupe(u8, ""));
}

/// Pop up to `limit` debounced refreshes due at or before `now_ms`.
/// Coalesces repeated entries for the same document.
pub fn planDrainDebounced(alloc: Allocator, reader: Reader, plan: *Plan, now_ms: u64, limit: usize) ![]InvalidationTarget {
    var out = std.ArrayListUnmanaged(InvalidationTarget).empty;
    errdefer {
        for (out.items) |t| {
            alloc.free(t.artifact);
            alloc.free(t.doc_key);
        }
        out.deinit(alloc);
    }
    const lower = familyPrefix(debounce_family);
    var upper_buf: [12]u8 = undefined;
    @memcpy(upper_buf[0..4], &lower);
    std.mem.writeInt(u64, upper_buf[4..12], now_ms +| 1, .big);
    var keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (keys.items) |k| alloc.free(k);
        keys.deinit(alloc);
    }
    const Collect = struct {
        alloc: Allocator,
        keys: *std.ArrayListUnmanaged([]u8),
        limit: usize,
        fn visit(p: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(p));
            try self.keys.append(self.alloc, try self.alloc.dupe(u8, key));
            return self.keys.items.len < self.limit;
        }
    };
    var collect = Collect{ .alloc = alloc, .keys = &keys, .limit = limit };
    try reader.scan(&lower, &upper_buf, .{ .ptr = &collect, .visit_fn = Collect.visit });
    for (keys.items) |key| {
        try plan.delete(key);
        var entry = (try parseDebounceKeyAlloc(alloc, key)) orelse continue;
        var dup = false;
        for (out.items) |t| if (std.mem.eql(u8, t.artifact, entry.artifact) and std.mem.eql(u8, t.doc_key, entry.doc_key)) {
            dup = true;
        };
        if (dup) {
            entry.deinit(alloc);
            continue;
        }
        try out.append(alloc, .{ .artifact = entry.artifact, .doc_key = entry.doc_key });
    }
    return try out.toOwnedSlice(alloc);
}

pub fn freeTargets(alloc: Allocator, targets: []InvalidationTarget) void {
    for (targets) |t| {
        alloc.free(t.artifact);
        alloc.free(t.doc_key);
    }
    alloc.free(targets);
}

// ---------------------------------------------------------------------------
// Inspection JSON
// ---------------------------------------------------------------------------

/// Public `freshness` object for artifact inspection.
pub fn freshnessJsonAlloc(alloc: Allocator, state: State) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"state\":");
    try ctx.appendJsonString(alloc, &out, @tagName(state.freshness));
    try out.print(alloc, ",\"generation\":{d},\"published_generation\":{d},\"version\":{d}", .{ state.generation, state.published_generation, state.published_version });
    if (state.frontier.len != 0) {
        try out.appendSlice(alloc, ",\"validated_through\":");
        try ctx.appendJsonString(alloc, &out, state.frontier);
    }
    if (state.published_generation != 0)
        try out.print(alloc, ",\"input_fingerprint\":\"sha256:{x}\"", .{&state.input_fingerprint});
    const rebuild: []const u8 = switch (state.freshness) {
        .current => "idle",
        .failed => "failed",
        else => if (state.rebuild_queued) "queued" else "manual",
    };
    try out.appendSlice(alloc, ",\"rebuild\":{\"state\":");
    try ctx.appendJsonString(alloc, &out, rebuild);
    if (state.failure.len != 0) {
        try out.appendSlice(alloc, ",\"error\":");
        try ctx.appendJsonString(alloc, &out, state.failure);
    }
    try out.appendSlice(alloc, "},\"reasons\":[");
    for (state.reasons, 0..) |r, i| {
        if (i != 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "{\"context_source\":");
        try ctx.appendJsonString(alloc, &out, r.source);
        try out.appendSlice(alloc, ",\"change\":");
        try ctx.appendJsonString(alloc, &out, r.change.jsonName());
        try out.appendSlice(alloc, ",\"table_id\":");
        try ctx.appendJsonString(alloc, &out, r.table_id);
        try out.appendSlice(alloc, ",\"key\":");
        try ctx.appendJsonString(alloc, &out, r.key);
        try out.append(alloc, '}');
    }
    try out.print(alloc, "],\"reasons_truncated\":{d}}}", .{state.dropped_reasons});
    return try out.toOwnedSlice(alloc);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Ordered in-memory store implementing `Reader` and applying plans.
pub const MemoryStore = struct {
    alloc: Allocator,
    map: std.ArrayListUnmanaged(Entry) = .empty,

    pub fn deinit(self: *MemoryStore) void {
        for (self.map.items) |e| {
            self.alloc.free(e.key);
            self.alloc.free(e.value);
        }
        self.map.deinit(self.alloc);
    }

    fn find(self: *const MemoryStore, key: []const u8) struct { index: usize, found: bool } {
        var lo: usize = 0;
        var hi: usize = self.map.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, self.map.items[mid].key, key)) {
                .eq => return .{ .index = mid, .found = true },
                .lt => lo = mid + 1,
                .gt => hi = mid,
            }
        }
        return .{ .index = lo, .found = false };
    }

    pub fn apply(self: *MemoryStore, plan: *const Plan) !void {
        for (plan.deletes.items) |key| {
            const at = self.find(key);
            if (at.found) {
                const e = self.map.orderedRemove(at.index);
                self.alloc.free(e.key);
                self.alloc.free(e.value);
            }
        }
        for (plan.puts.items) |e| {
            const at = self.find(e.key);
            const value = try self.alloc.dupe(u8, e.value);
            if (at.found) {
                self.alloc.free(self.map.items[at.index].value);
                self.map.items[at.index].value = value;
            } else {
                try self.map.insert(self.alloc, at.index, .{ .key = try self.alloc.dupe(u8, e.key), .value = value });
            }
        }
    }

    pub fn reader(self: *MemoryStore) Reader {
        return .{ .ptr = self, .get_fn = getImpl, .scan_fn = scanImpl };
    }

    fn getImpl(p: *anyopaque, alloc: Allocator, key: []const u8) anyerror!?[]u8 {
        const self: *MemoryStore = @ptrCast(@alignCast(p));
        const at = self.find(key);
        if (!at.found) return null;
        return try alloc.dupe(u8, self.map.items[at.index].value);
    }

    fn scanImpl(p: *anyopaque, lower: []const u8, upper: []const u8, visitor: Reader.Visitor) anyerror!void {
        const self: *MemoryStore = @ptrCast(@alignCast(p));
        var i = self.find(lower).index;
        while (i < self.map.items.len) : (i += 1) {
            const e = self.map.items[i];
            if (!std.mem.lessThan(u8, e.key, upper)) return;
            if (!try visitor.visit_fn(visitor.ptr, e.key, e.value)) return;
        }
    }

    pub fn countPrefix(self: *const MemoryStore, prefix: []const u8) usize {
        var n: usize = 0;
        for (self.map.items) |e| {
            if (std.mem.startsWith(u8, e.key, prefix)) n += 1;
        }
        return n;
    }
};

fn testTuple(byte: u8) ctx.SubscriptionTuple {
    return .{ .spec = @splat(0xaa), .tuple = @splat(byte) };
}

fn commit(store: *MemoryStore, plan: *Plan) !void {
    try store.apply(plan);
    plan.deinit();
    plan.* = Plan.init(store.alloc);
}

test "state record round trips" {
    const alloc = testing.allocator;
    const reasons = [_]Reason{.{ .source = "policies", .change = .member_added, .table_id = "t7", .key = "p-1" }};
    const tuples = [_]ctx.SubscriptionTuple{ testTuple(1), testTuple(2) };
    const raw = try encodeStateAlloc(alloc, .{ .generation = 9, .published_generation = 8, .published_version = 3, .freshness = .stale, .rebuild_queued = true, .frontier = "{\"t7\":\"42\"}", .tuples = &tuples, .reasons = &reasons, .dropped_reasons = 2 });
    defer alloc.free(raw);
    var state = try decodeStateAlloc(alloc, raw);
    defer state.deinit(alloc);
    try testing.expectEqual(@as(u64, 9), state.generation);
    try testing.expectEqual(Freshness.stale, state.freshness);
    try testing.expect(state.rebuild_queued);
    try testing.expectEqualStrings("p-1", state.reasons[0].key);
    try testing.expect(state.hasTuple(testTuple(2)));
    try testing.expect(!state.hasTuple(testTuple(3)));
    try testing.expectError(error.InvalidDependencyState, decodeStateAlloc(alloc, raw[0 .. raw.len - 1]));
}

test "register, invalidate, and fence publication on dependency generation" {
    const alloc = testing.allocator;
    var store = MemoryStore{ .alloc = alloc };
    defer store.deinit();
    var plan = Plan.init(alloc);
    defer plan.deinit();
    const digest: Digest = @splat(7);
    const t1 = testTuple(1);

    const capture = try planRegister(alloc, store.reader(), &plan, "doc-a", "summary", &.{t1}, digest);
    try commit(&store, &plan);
    try testing.expectEqual(@as(usize, 1), store.countPrefix(&subscriptionTuplePrefix(t1.spec, t1.tuple)));

    // An input change lands between the context read and publication.
    var page = try planInvalidateTuple(alloc, store.reader(), &plan, t1.spec, t1.tuple, .{ .source = "policies", .change = .member_added, .table_id = "t7", .key = "p-9" });
    defer page.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), page.targets.items.len);
    try testing.expectEqualStrings("doc-a", page.targets.items[0].doc_key);
    try commit(&store, &plan);

    // The superseded computation cannot publish.
    try testing.expectError(error.DependencyGenerationSuperseded, planPublish(alloc, store.reader(), &plan, "doc-a", "summary", .{ .capture = capture, .tuples = &.{t1}, .input_fingerprint = @splat(1) }));

    // The re-queued computation re-registers (idempotent) and publishes.
    const capture2 = try planRegister(alloc, store.reader(), &plan, "doc-a", "summary", &.{t1}, digest);
    try testing.expectEqual(capture.generation + 1, capture2.generation);
    try commit(&store, &plan);
    const inputs = [_]ManifestInput{.{ .source = "policies", .table_id = "t7", .key = "p-9", .revision = @splat(3) }};
    const result = try planPublish(alloc, store.reader(), &plan, "doc-a", "summary", .{ .capture = capture2, .tuples = &.{t1}, .input_fingerprint = @splat(1), .inputs = &inputs, .frontier = "{\"t7\":\"5\"}" });
    try testing.expectEqual(@as(u64, 1), result.version);
    try commit(&store, &plan);
    var state = (try loadState(alloc, store.reader(), null, "doc-a", "summary")).?;
    defer state.deinit(alloc);
    try testing.expectEqual(Freshness.current, state.freshness);
    try testing.expectEqual(@as(usize, 0), state.reasons.len);
    const fresh = try freshnessJsonAlloc(alloc, state);
    defer alloc.free(fresh);
    try testing.expect(std.mem.indexOf(u8, fresh, "\"state\":\"current\"") != null);
}

test "rebinding keeps old subscriptions until publication replaces them" {
    const alloc = testing.allocator;
    var store = MemoryStore{ .alloc = alloc };
    defer store.deinit();
    var plan = Plan.init(alloc);
    defer plan.deinit();
    const digest: Digest = @splat(7);
    const old_t = testTuple(1);
    const new_t = testTuple(2);
    const c1 = try planRegister(alloc, store.reader(), &plan, "doc", "a", &.{old_t}, digest);
    try commit(&store, &plan);
    _ = try planPublish(alloc, store.reader(), &plan, "doc", "a", .{ .capture = c1, .tuples = &.{old_t}, .input_fingerprint = @splat(1) });
    try commit(&store, &plan);

    // Source row now binds a different organization.
    const c2 = try planRegister(alloc, store.reader(), &plan, "doc", "a", &.{new_t}, digest);
    try commit(&store, &plan);
    // While in flight, both old and new tuples deliver invalidations.
    try testing.expectEqual(@as(usize, 1), store.countPrefix(&subscriptionTuplePrefix(old_t.spec, old_t.tuple)));
    try testing.expectEqual(@as(usize, 1), store.countPrefix(&subscriptionTuplePrefix(new_t.spec, new_t.tuple)));
    _ = try planPublish(alloc, store.reader(), &plan, "doc", "a", .{ .capture = c2, .tuples = &.{new_t}, .input_fingerprint = @splat(2) });
    try commit(&store, &plan);
    try testing.expectEqual(@as(usize, 0), store.countPrefix(&subscriptionTuplePrefix(old_t.spec, old_t.tuple)));
    try testing.expectEqual(@as(usize, 1), store.countPrefix(&subscriptionTuplePrefix(new_t.spec, new_t.tuple)));

    // A stale hint (left behind by a crash or split) is verified and collected.
    try plan.putOwned(try subscriptionKeyAlloc(alloc, old_t.spec, old_t.tuple, "a", "doc"), try alloc.dupe(u8, ""));
    try commit(&store, &plan);
    var page = try planInvalidateTuple(alloc, store.reader(), &plan, old_t.spec, old_t.tuple, .{ .source = "s", .change = .member_changed, .table_id = "t", .key = "k" });
    defer page.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), page.targets.items.len);
    try commit(&store, &plan);
    try testing.expectEqual(@as(usize, 0), store.countPrefix(&subscriptionTuplePrefix(old_t.spec, old_t.tuple)));
}

test "fan-out invalidation is paged, range-filtered, and coalesces reasons" {
    const alloc = testing.allocator;
    var store = MemoryStore{ .alloc = alloc };
    defer store.deinit();
    var plan = Plan.init(alloc);
    defer plan.deinit();
    const t = testTuple(9);
    var buf: [16]u8 = undefined;
    for (0..25) |i| {
        const doc = try std.fmt.bufPrint(&buf, "doc-{d:0>3}", .{i});
        _ = try planRegister(alloc, store.reader(), &plan, doc, "a", &.{t}, @splat(1));
    }
    try commit(&store, &plan);
    var total: usize = 0;
    var resume_key: ?[]u8 = null;
    defer if (resume_key) |r| alloc.free(r);
    var pages: usize = 0;
    while (true) {
        var page = try planInvalidateTuple(alloc, store.reader(), &plan, t.spec, t.tuple, .{
            .source = "s",
            .change = .member_added,
            .table_id = "t",
            .key = "k",
            .max_targets = 10,
            .resume_after = resume_key orelse "",
            .range_upper = "doc-020",
        });
        defer page.deinit(alloc);
        total += page.targets.items.len;
        pages += 1;
        try commit(&store, &plan);
        if (resume_key) |r| alloc.free(r);
        resume_key = page.resume_after;
        page.resume_after = null;
        if (resume_key == null) break;
    }
    try testing.expectEqual(@as(usize, 20), total);
    try testing.expectEqual(@as(usize, 3), pages);
    // Identical reasons coalesce; distinct ones are bounded.
    for (0..12) |i| {
        var kb: [8]u8 = undefined;
        _ = try planInvalidateDocument(alloc, store.reader(), &plan, "doc-001", "a", .{ .source = "s", .change = .member_changed, .table_id = "t", .key = try std.fmt.bufPrint(&kb, "k{d}", .{i}) });
        try commit(&store, &plan);
    }
    var state = (try loadState(alloc, store.reader(), null, "doc-001", "a")).?;
    defer state.deinit(alloc);
    try testing.expectEqual(@as(usize, max_reasons), state.reasons.len);
    try testing.expect(state.dropped_reasons >= 4);
}

test "versioned manifests are retained, pruned with their impact hints, and confirmed on lookup" {
    const alloc = testing.allocator;
    var store = MemoryStore{ .alloc = alloc };
    defer store.deinit();
    var plan = Plan.init(alloc);
    defer plan.deinit();
    const t = testTuple(4);
    var revisions: [5]Digest = undefined;
    for (&revisions, 0..) |*r, i| r.* = @splat(@intCast(i + 10));
    for (revisions) |rev| {
        const c = try planRegister(alloc, store.reader(), &plan, "doc", "summary", &.{t}, @splat(1));
        try commit(&store, &plan);
        const inputs = [_]ManifestInput{.{ .source = "policies", .table_id = "t7", .key = "p-1", .revision = rev }};
        _ = try planPublish(alloc, store.reader(), &plan, "doc", "summary", .{ .capture = c, .tuples = &.{t}, .input_fingerprint = rev, .inputs = &inputs, .retain_versions = 3 });
        try commit(&store, &plan);
        _ = try planInvalidateDocument(alloc, store.reader(), &plan, "doc", "summary", .{ .source = "policies", .change = .member_changed, .table_id = "t7", .key = "p-1" });
        try commit(&store, &plan);
    }
    const hprefix = try historyPrefixAlloc(alloc, "doc", "summary");
    defer alloc.free(hprefix);
    try testing.expectEqual(@as(usize, 3), store.countPrefix(hprefix));

    // Pruned revision: no rows. Retained revision: exactly its version.
    var pruned = try queryImpact(alloc, store.reader(), "t7", "p-1", revisions[0], "", 100, "", "");
    defer pruned.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), pruned.rows.items.len);
    var retained = try queryImpact(alloc, store.reader(), "t7", "p-1", revisions[3], "", 100, "", "");
    defer retained.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), retained.rows.items.len);
    try testing.expectEqual(@as(u64, 4), retained.rows.items[0].version);
    try testing.expect(!retained.rows.items[0].current_version);
    var all = try queryImpact(alloc, store.reader(), "t7", "p-1", null, "", 2, "", "");
    defer all.deinit(alloc);
    try testing.expectEqual(@as(usize, 2), all.rows.items.len);
    try testing.expect(all.resume_after != null);
    var rest = try queryImpact(alloc, store.reader(), "t7", "p-1", null, all.resume_after.?, 2, "", "");
    defer rest.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), rest.rows.items.len);
    try testing.expect(rest.resume_after == null);
    try testing.expect(rest.rows.items[0].current_version);
    try testing.expectEqual(Freshness.stale, rest.rows.items[0].freshness);

    // A fabricated impact hint without a confirming manifest is ignored.
    try plan.putOwned(try impactKeyAlloc(alloc, "t7", "p-1", @splat(0x99), "summary", "doc", 2), try alloc.dupe(u8, ""));
    try commit(&store, &plan);
    var fabricated = try queryImpact(alloc, store.reader(), "t7", "p-1", @as(Digest, @splat(0x99)), "", 100, "", "");
    defer fabricated.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), fabricated.rows.items.len);

    // Cleanup with erasure removes state, subscriptions, manifests, and hints.
    try planCleanup(alloc, store.reader(), &plan, "doc", "summary", true);
    try commit(&store, &plan);
    try testing.expectEqual(@as(usize, 0), store.countPrefix(hprefix));
    try testing.expectEqual(@as(usize, 0), store.countPrefix(&subscriptionTuplePrefix(t.spec, t.tuple)));
    var after = try queryImpact(alloc, store.reader(), "t7", "p-1", null, "", 100, "", "");
    defer after.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), after.rows.items.len);
}

test "failure keeps the last artifact non-current and configuration changes advance the generation" {
    const alloc = testing.allocator;
    var store = MemoryStore{ .alloc = alloc };
    defer store.deinit();
    var plan = Plan.init(alloc);
    defer plan.deinit();
    const t = testTuple(5);
    const c1 = try planRegister(alloc, store.reader(), &plan, "doc", "a", &.{t}, @splat(1));
    try commit(&store, &plan);
    _ = try planPublish(alloc, store.reader(), &plan, "doc", "a", .{ .capture = c1, .tuples = &.{t}, .input_fingerprint = @splat(1) });
    try commit(&store, &plan);
    _ = try planInvalidateDocument(alloc, store.reader(), &plan, "doc", "a", .{ .source = "s", .change = .document_changed, .table_id = "t", .key = "k" });
    try commit(&store, &plan);
    const c2 = try planRegister(alloc, store.reader(), &plan, "doc", "a", &.{t}, @splat(1));
    try commit(&store, &plan);
    try planFailure(alloc, store.reader(), &plan, "doc", "a", c2, "ContextBudgetExceeded");
    try commit(&store, &plan);
    var state = (try loadState(alloc, store.reader(), null, "doc", "a")).?;
    defer state.deinit(alloc);
    try testing.expectEqual(Freshness.failed, state.freshness);
    try testing.expectEqual(@as(u64, 1), state.published_version);
    try testing.expectEqualStrings("ContextBudgetExceeded", state.failure);

    // A new declaration supersedes in-flight work under the old one.
    const c3 = try planRegister(alloc, store.reader(), &plan, "doc", "a", &.{t}, @splat(2));
    try commit(&store, &plan);
    try testing.expect(c3.generation > c2.generation);
    try testing.expectError(error.DependencyGenerationSuperseded, planPublish(alloc, store.reader(), &plan, "doc", "a", .{ .capture = c2, .tuples = &.{t}, .input_fingerprint = @splat(1) }));
}

test "range hint copy and debounced refresh queue" {
    const alloc = testing.allocator;
    var store = MemoryStore{ .alloc = alloc };
    defer store.deinit();
    var plan = Plan.init(alloc);
    defer plan.deinit();
    const t = testTuple(6);
    for ([_][]const u8{ "a", "m", "z" }) |doc| {
        const c = try planRegister(alloc, store.reader(), &plan, doc, "x", &.{t}, @splat(1));
        try commit(&store, &plan);
        const inputs = [_]ManifestInput{.{ .source = "s", .table_id = "t", .key = "k", .revision = @splat(1) }};
        _ = try planPublish(alloc, store.reader(), &plan, doc, "x", .{ .capture = c, .tuples = &.{t}, .input_fingerprint = @splat(1), .inputs = &inputs });
        try commit(&store, &plan);
    }
    var dest = MemoryStore{ .alloc = alloc };
    defer dest.deinit();
    var dest_plan = Plan.init(alloc);
    defer dest_plan.deinit();
    const copied = try copyHintsForRange(alloc, store.reader(), &dest_plan, "m", "");
    try testing.expectEqual(@as(usize, 4), copied); // 2 subscriptions + 2 impact hints
    try dest.apply(&dest_plan);
    try testing.expectEqual(@as(usize, 2), dest.countPrefix(&subscriptionTuplePrefix(t.spec, t.tuple)));

    try planQueueDebounced(&plan, 1000, "x", "a");
    try planQueueDebounced(&plan, 1500, "x", "a");
    try planQueueDebounced(&plan, 5000, "x", "m");
    try commit(&store, &plan);
    const due = try planDrainDebounced(alloc, store.reader(), &plan, 2000, 16);
    defer freeTargets(alloc, due);
    try testing.expectEqual(@as(usize, 1), due.len);
    try testing.expectEqualStrings("a", due[0].doc_key);
    try commit(&store, &plan);
    try testing.expectEqual(@as(usize, 1), store.countPrefix(&familyPrefix(debounce_family)));
}

test "subscription and impact key parsing round trips escaped components" {
    const alloc = testing.allocator;
    const doc = "doc\x00with\x00nul";
    const key = try subscriptionKeyAlloc(alloc, @splat(1), @splat(2), "art\x00", doc);
    defer alloc.free(key);
    var hint = (try parseSubscriptionKeyAlloc(alloc, key)).?;
    defer hint.deinit(alloc);
    try testing.expectEqualStrings(doc, hint.doc_key);
    try testing.expectEqualStrings("art\x00", hint.artifact);
    const ikey = try impactKeyAlloc(alloc, "t\x00", "in", @splat(3), "art", doc, 77);
    defer alloc.free(ikey);
    var impact = (try parseImpactKeyAlloc(alloc, ikey)).?;
    defer impact.deinit(alloc);
    try testing.expectEqual(@as(u64, 77), impact.version);
    try testing.expectEqualStrings("t\x00", impact.table_id);
    try testing.expectEqualStrings(doc, impact.doc_key);
}
