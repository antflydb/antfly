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

//! Change notices: how an input-table write reaches dependent artifacts.
//!
//! Source side (the table being written)
//! -------------------------------------
//! A table that is the target of any context source keeps a *shadow* per row:
//! the canonical projection of only the fields its dependents declare
//! (predicate and projected fields). In the same atomic batch as a write or
//! delete, `planStageNotices` compares the shadow (previous) with the new row
//! (next), computes the subscription tuples touched for every dependent spec
//! (`context_sources.touchedTuplesAlloc`), updates the shadow, and stages
//! compact notices in a durable outbox. Irrelevant updates (equal projection
//! and tuples) stage nothing, so unrelated tenants and unconsumed fields never
//! reach dependents. A row written before its spec was registered has no
//! covering shadow; it stages a spec-wide notice (safe over-invalidation)
//! until `planBackfillShadows` has covered the table.
//!
//! Delivery
//! --------
//! Outbox entries are drained after commit (and on open/maintenance) to the
//! dependent table: locally when it is the same DB, or through the host's
//! dispatcher. An entry is deleted only after the dependent applied it, so a
//! crash, restart, leader transfer, or failed dispatch re-delivers; applying
//! is idempotent (it only advances generations and coalesces reasons).
//!
//! Dependent side
//! --------------
//! `planApplyNotice` performs the indexed subscription lookup (bounded pages)
//! and returns the dependents to rebuild now, to queue with debounce, and a
//! continuation notice when the fan-out page budget was reached. Reporting
//! invalidation (stale freshness, generation bump) is never debounced; only
//! the rebuild is.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ctx = @import("context_sources.zig");
const store = @import("context_dependency_store.zig");

pub const Digest = ctx.Digest;
pub const shadow_kind: u8 = 0x5f;
pub const outbox_family: u8 = 0x04;
pub const notice_format: u32 = 1;
pub const default_fanout_page: usize = 512;

// ---------------------------------------------------------------------------
// Specs that target a source table
// ---------------------------------------------------------------------------

pub const SpecBinding = struct {
    dependent_table_id: []const u8,
    artifact: []const u8,
    source: *const ctx.Source,
    spec: Digest,
    refresh: ctx.Refresh,
};

pub const DeclaredConfig = struct {
    /// Dependent table identity ("" for this DB's own enrichments).
    dependent_table_id: []const u8,
    artifact: []const u8,
    /// Canonical stored context-sources JSON.
    stored_json: []const u8,
};

/// Every context source, across own and inbound dependents, that reads this
/// table — plus the union of fields the shadow must capture.
pub const SourceSpecs = struct {
    alloc: Allocator,
    configs: []ctx.Config,
    items: []SpecBinding,
    union_fields: []const []const u8,
    fields_digest: Digest,
    /// Generated artifacts read by any spec (sorted, unique).
    artifact_names: []const []const u8 = &.{},
    /// Some graph spec traverses field-derived or generated edges.
    generated_edges: bool = false,

    pub fn deinit(self: *SourceSpecs) void {
        for (self.configs) |*c| c.deinit();
        self.alloc.free(self.configs);
        self.alloc.free(self.items);
        self.alloc.free(self.union_fields);
        self.alloc.free(self.artifact_names);
        self.* = undefined;
    }

    pub fn isEmpty(self: *const SourceSpecs) bool {
        return self.items.len == 0;
    }
};

/// `own_table_id` identifies this table; own enrichments' sources with an
/// empty identity also target it.
pub fn buildSourceSpecs(alloc: Allocator, own_table_id: []const u8, declared: []const DeclaredConfig) !SourceSpecs {
    var configs = std.ArrayListUnmanaged(ctx.Config).empty;
    errdefer {
        for (configs.items) |*c| c.deinit();
        configs.deinit(alloc);
    }
    for (declared) |d| {
        const config = ctx.parseStoredJson(alloc, d.stored_json) catch |err| switch (err) {
            error.OutOfMemory => return err,
            // A malformed inbound declaration must not block writes to this
            // table; the dependent's own admission rejects it.
            else => continue,
        };
        try configs.append(alloc, config);
    }
    var items = std.ArrayListUnmanaged(SpecBinding).empty;
    errdefer items.deinit(alloc);
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    defer fields.deinit(alloc);
    var config_index: usize = 0;
    for (declared) |d| {
        if (ctx.parseStoredJson(alloc, d.stored_json)) |probe| {
            var p = probe;
            p.deinit();
        } else |_| continue;
        const config = &configs.items[config_index];
        config_index += 1;
        for (config.sources) |*source| {
            const targets_this = if (source.table_id.len == 0)
                d.dependent_table_id.len == 0 or std.mem.eql(u8, d.dependent_table_id, own_table_id)
            else
                std.mem.eql(u8, source.table_id, own_table_id);
            // Snapshot sources register no subscriptions; ranked retrieval
            // is never invalidated by row changes.
            if (!targets_this or !source.isLive() or source.kind == .search) continue;
            try items.append(alloc, .{
                .dependent_table_id = d.dependent_table_id,
                .artifact = d.artifact,
                .source = source,
                .spec = source.specDigest(),
                .refresh = config.refresh,
            });
            for (source.relevant_fields) |f| try fields.append(alloc, f);
            for (source.conjuncts) |c| try fields.append(alloc, c.field);
        }
    }
    std.mem.sort([]const u8, fields.items, {}, struct {
        fn lt(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.lt);
    var unique = std.ArrayListUnmanaged([]const u8).empty;
    errdefer unique.deinit(alloc);
    for (fields.items) |f| {
        if (unique.items.len != 0 and std.mem.eql(u8, unique.items[unique.items.len - 1], f)) continue;
        try unique.append(alloc, f);
    }
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    // Shadows are nested (tree) projections; the marker invalidates flat ones.
    h.update("shadow.tree.v2");
    for (unique.items) |f| {
        h.update(f);
        h.update(&.{0});
    }
    var digest: Digest = undefined;
    h.final(&digest);
    var artifacts = std.ArrayListUnmanaged([]const u8).empty;
    errdefer artifacts.deinit(alloc);
    var generated_edges = false;
    for (items.items) |binding| {
        for (binding.source.artifact_names) |name| {
            var seen = false;
            for (artifacts.items) |existing| if (std.mem.eql(u8, existing, name)) {
                seen = true;
            };
            if (!seen) try artifacts.append(alloc, name);
        }
        if (binding.source.kind == .graph and binding.source.graph.include_generated) generated_edges = true;
    }
    std.mem.sort([]const u8, artifacts.items, {}, struct {
        fn lt(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.lt);
    return .{
        .alloc = alloc,
        .configs = try configs.toOwnedSlice(alloc),
        .items = try items.toOwnedSlice(alloc),
        .union_fields = try unique.toOwnedSlice(alloc),
        .fields_digest = digest,
        .artifact_names = try artifacts.toOwnedSlice(alloc),
        .generated_edges = generated_edges,
    };
}

// ---------------------------------------------------------------------------
// Keys and records
// ---------------------------------------------------------------------------

pub fn shadowKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try list.append(alloc, store.user_namespace);
    for (doc_key) |b| {
        if (b == 0) try list.appendSlice(alloc, &.{ 0, 0xff }) else try list.append(alloc, b);
    }
    try list.appendSlice(alloc, &.{ 0, 0, shadow_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn outboxKey(sequence: u64, ordinal: u32) [16]u8 {
    var out: [16]u8 = undefined;
    @memcpy(out[0..4], &store.familyPrefix(outbox_family));
    std.mem.writeInt(u64, out[4..12], sequence, .big);
    std.mem.writeInt(u32, out[12..16], ordinal, .big);
    return out;
}

pub const Notice = struct {
    dependent_table_id: []const u8,
    artifact: []const u8,
    source_name: []const u8,
    source_table_id: []const u8,
    key: []const u8,
    spec: Digest,
    tuple: Digest = @splat(0),
    /// Spec-wide: every subscriber of `spec` (tuple ignored).
    all: bool = false,
    change: ctx.ChangeKind,
    refresh: ctx.Refresh,
    sequence: u64,
    /// Fan-out continuation cursor (subscription key) when non-empty.
    resume_after: []const u8 = "",
    /// Residual sources: the row's relevant projection before/after the
    /// change ("" absent, "?" unknown), evaluated against each subscriber's
    /// parameters on the dependent side.
    prev_row: []const u8 = "",
    next_row: []const u8 = "",
    /// Further tuples touched by the same row change (range-aware specs
    /// touch a value's ancestor blocks); processed after `tuple` in order.
    more_tuples: []const Digest = &.{},
};

pub fn encodeNoticeAlloc(alloc: Allocator, n: Notice) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.print(alloc, "{{\"format\":{d},\"dependent_table_id\":", .{notice_format});
    try ctx.appendJsonString(alloc, &out, n.dependent_table_id);
    try out.appendSlice(alloc, ",\"artifact\":");
    try ctx.appendJsonString(alloc, &out, n.artifact);
    try out.appendSlice(alloc, ",\"source\":");
    try ctx.appendJsonString(alloc, &out, n.source_name);
    try out.appendSlice(alloc, ",\"source_table_id\":");
    try ctx.appendJsonString(alloc, &out, n.source_table_id);
    try out.appendSlice(alloc, ",\"key\":");
    try ctx.appendJsonString(alloc, &out, n.key);
    try out.print(alloc, ",\"spec\":\"{x}\",\"tuple\":\"{x}\",\"all\":{},\"change\":\"{s}\",\"refresh\":\"{s}\",\"debounce_ms\":{d},\"sequence\":{d},\"resume_after\":\"{x}\",\"prev_row\":", .{
        &n.spec, &n.tuple, n.all, @tagName(n.change), @tagName(n.refresh.mode), n.refresh.debounce_ms, n.sequence, n.resume_after,
    });
    try ctx.appendJsonString(alloc, &out, n.prev_row);
    try out.appendSlice(alloc, ",\"next_row\":");
    try ctx.appendJsonString(alloc, &out, n.next_row);
    try out.appendSlice(alloc, ",\"more_tuples\":\"");
    for (n.more_tuples) |t| try out.print(alloc, "{x}", .{&t});
    try out.appendSlice(alloc, "\"}");
    return try out.toOwnedSlice(alloc);
}

pub const DecodedNotice = struct {
    arena: std.heap.ArenaAllocator,
    notice: Notice,

    pub fn deinit(self: *DecodedNotice) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn decodeNoticeAlloc(alloc: Allocator, raw: []const u8) !DecodedNotice {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    const value = std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{}) catch return error.InvalidContextNotice;
    if (value != .object) return error.InvalidContextNotice;
    const o = value.object;
    const str = struct {
        fn get(obj: std.json.ObjectMap, name: []const u8) ![]const u8 {
            const v = obj.get(name) orelse return error.InvalidContextNotice;
            if (v != .string) return error.InvalidContextNotice;
            return v.string;
        }
        fn int(obj: std.json.ObjectMap, name: []const u8) !u64 {
            const v = obj.get(name) orelse return error.InvalidContextNotice;
            if (v != .integer or v.integer < 0) return error.InvalidContextNotice;
            return @intCast(v.integer);
        }
        fn digest(obj: std.json.ObjectMap, name: []const u8) !Digest {
            const hex = try get(obj, name);
            if (hex.len != 64) return error.InvalidContextNotice;
            var out: Digest = undefined;
            _ = std.fmt.hexToBytes(&out, hex) catch return error.InvalidContextNotice;
            return out;
        }
    };
    if (try str.int(o, "format") != notice_format) return error.InvalidContextNotice;
    const resume_hex = try str.get(o, "resume_after");
    const resume_after = try a.alloc(u8, resume_hex.len / 2);
    _ = std.fmt.hexToBytes(resume_after, resume_hex) catch return error.InvalidContextNotice;
    const all = o.get("all") orelse return error.InvalidContextNotice;
    if (all != .bool) return error.InvalidContextNotice;
    return .{ .arena = arena, .notice = .{
        .dependent_table_id = try str.get(o, "dependent_table_id"),
        .artifact = try str.get(o, "artifact"),
        .source_name = try str.get(o, "source"),
        .source_table_id = try str.get(o, "source_table_id"),
        .key = try str.get(o, "key"),
        .spec = try str.digest(o, "spec"),
        .tuple = try str.digest(o, "tuple"),
        .all = all.bool,
        .change = std.meta.stringToEnum(ctx.ChangeKind, try str.get(o, "change")) orelse return error.InvalidContextNotice,
        .refresh = .{
            .mode = std.meta.stringToEnum(ctx.RefreshMode, try str.get(o, "refresh")) orelse return error.InvalidContextNotice,
            .debounce_ms = try str.int(o, "debounce_ms"),
        },
        .sequence = try str.int(o, "sequence"),
        .resume_after = resume_after,
        .prev_row = if (o.get("prev_row")) |v| (if (v == .string) v.string else "") else "",
        .next_row = if (o.get("next_row")) |v| (if (v == .string) v.string else "") else "",
        .more_tuples = blk: {
            const v = o.get("more_tuples") orelse break :blk &.{};
            if (v != .string or v.string.len % 64 != 0) return error.InvalidContextNotice;
            const list = try a.alloc(Digest, v.string.len / 64);
            for (list, 0..) |*d, i| _ = std.fmt.hexToBytes(d, v.string[i * 64 ..][0..64]) catch return error.InvalidContextNotice;
            break :blk list;
        },
    } };
}

// ---------------------------------------------------------------------------
// Source side: shadow + outbox staging
// ---------------------------------------------------------------------------

pub const RowChange = struct {
    key: []const u8,
    /// Logical JSON of the new row; null for a delete (including TTL expiry).
    next: ?[]const u8,
};

pub const StageResult = struct {
    notices: usize = 0,
    next_ordinal: u32 = 0,
};

/// Shadow value: fields digest (32 bytes) followed by the canonical
/// projection JSON of the union fields.
fn encodeShadowAlloc(alloc: Allocator, digest: Digest, projection: []const u8) ![]u8 {
    return std.mem.concat(alloc, u8, &.{ &digest, projection });
}

/// Stage notices for a batch of row changes. `sequence` orders the outbox;
/// `first_ordinal` lets several stages share one sequence.
pub fn planStageNotices(
    alloc: Allocator,
    reader: store.Reader,
    plan: *store.Plan,
    specs: *const SourceSpecs,
    own_table_id: []const u8,
    sequence: u64,
    first_ordinal: u32,
    changes: []const RowChange,
    shadows_complete: bool,
) !StageResult {
    var result = StageResult{ .next_ordinal = first_ordinal };
    if (specs.isEmpty()) return result;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    for (changes) |change| {
        _ = arena.reset(.retain_capacity);
        const a = arena.allocator();
        const skey = try shadowKeyAlloc(a, change.key);
        const previous_raw: ?[]const u8 = switch (plan.pending(skey)) {
            .put => |v| v,
            .deleted => null,
            .untouched => try reader.get(a, skey),
        };
        var previous: ?std.json.Value = null;
        var previous_unknown = false;
        if (previous_raw) |raw| {
            if (raw.len >= 32 and std.mem.eql(u8, raw[0..32], &specs.fields_digest)) {
                previous = std.json.parseFromSliceLeaky(std.json.Value, a, raw[32..], .{}) catch null;
                if (previous == null) previous_unknown = true;
            } else previous_unknown = true;
        } else if (!shadows_complete) {
            // Pre-registration row (or backfill still running): its previous
            // contribution is unknown, so notify spec-wide.
            previous_unknown = true;
        }
        var next: ?std.json.Value = null;
        var next_projection: ?[]u8 = null;
        if (change.next) |raw| {
            const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{}) catch null;
            if (parsed) |row| if (row == .object) {
                next_projection = try ctx.projectTreeAlloc(a, row, specs.union_fields);
                next = try std.json.parseFromSliceLeaky(std.json.Value, a, next_projection.?, .{});
            };
        }
        if (!previous_unknown and previous == null and next == null) continue;
        if (!previous_unknown and previous != null and next_projection != null and previous_raw != null and
            std.mem.eql(u8, previous_raw.?[32..], next_projection.?)) continue;

        for (specs.items) |binding| {
            const residual = binding.source.hasResidual();
            const prev_row: []const u8 = if (!residual) "" else if (previous_unknown) "?" else if (previous) |row| try ctx.projectTreeAlloc(a, row, binding.source.relevant_fields) else "";
            const next_row: []const u8 = if (!residual) "" else if (next) |row| try ctx.projectTreeAlloc(a, row, binding.source.relevant_fields) else "";
            if (previous_unknown) {
                const change_kind: ctx.ChangeKind = if (binding.source.kind == .document)
                    (if (next == null) .document_deleted else .document_changed)
                else
                    (if (next == null) .member_removed else .member_changed);
                if (binding.source.kind == .document or binding.source.kind == .graph) {
                    // Document subscriptions are keyed by the row key, so the
                    // exact tuple is known even without a shadow.
                    try stage(alloc, plan, binding, own_table_id, change.key, ctx.documentTuple(binding.spec, change.key), false, change_kind, sequence, &result);
                } else {
                    try stageRows(alloc, plan, binding, own_table_id, change.key, @splat(0), true, change_kind, sequence, &result, prev_row, next_row);
                }
                continue;
            }
            var touched = try ctx.touchedTuplesAlloc(a, binding.source, change.key, previous, next);
            defer touched.deinit(a);
            if (touched.all) {
                try stageRows(alloc, plan, binding, own_table_id, change.key, @splat(0), true, touched.all_change, sequence, &result, prev_row, next_row);
                continue;
            }
            if (touched.tuples.len == 0) continue;
            // One notice per row change and spec, carrying every touched tuple.
            var kind = touched.tuples[0].change;
            const more = try a.alloc(Digest, touched.tuples.len - 1);
            for (touched.tuples[1..], more) |t, *d| {
                d.* = t.tuple;
                if (t.change != kind) kind = .member_changed;
            }
            try stageMany(alloc, plan, binding, own_table_id, change.key, touched.tuples[0].tuple, more, kind, sequence, &result, prev_row, next_row);
        }

        if (next_projection) |projection| {
            try plan.putOwned(try alloc.dupe(u8, skey), try encodeShadowAlloc(alloc, specs.fields_digest, projection));
        } else {
            try plan.delete(skey);
        }
    }
    return result;
}

fn stage(alloc: Allocator, plan: *store.Plan, binding: SpecBinding, own_table_id: []const u8, key: []const u8, tuple: Digest, all: bool, change: ctx.ChangeKind, sequence: u64, result: *StageResult) !void {
    return stageRows(alloc, plan, binding, own_table_id, key, tuple, all, change, sequence, result, "", "");
}

fn stageMany(alloc: Allocator, plan: *store.Plan, binding: SpecBinding, own_table_id: []const u8, key: []const u8, tuple: Digest, more: []const Digest, change: ctx.ChangeKind, sequence: u64, result: *StageResult, prev_row: []const u8, next_row: []const u8) !void {
    const value = try encodeNoticeAlloc(alloc, .{
        .more_tuples = more,
        .prev_row = prev_row,
        .next_row = next_row,
        .dependent_table_id = binding.dependent_table_id,
        .artifact = binding.artifact,
        .source_name = binding.source.name,
        .source_table_id = own_table_id,
        .key = key,
        .spec = binding.spec,
        .tuple = tuple,
        .change = change,
        .refresh = binding.refresh,
        .sequence = sequence,
    });
    errdefer alloc.free(value);
    const okey = outboxKey(sequence, result.next_ordinal);
    try plan.putOwned(try alloc.dupe(u8, &okey), value);
    result.next_ordinal += 1;
    result.notices += 1;
}

fn stageRows(alloc: Allocator, plan: *store.Plan, binding: SpecBinding, own_table_id: []const u8, key: []const u8, tuple: Digest, all: bool, change: ctx.ChangeKind, sequence: u64, result: *StageResult, prev_row: []const u8, next_row: []const u8) !void {
    const value = try encodeNoticeAlloc(alloc, .{
        .prev_row = prev_row,
        .next_row = next_row,
        .dependent_table_id = binding.dependent_table_id,
        .artifact = binding.artifact,
        .source_name = binding.source.name,
        .source_table_id = own_table_id,
        .key = key,
        .spec = binding.spec,
        .tuple = tuple,
        .all = all,
        .change = change,
        .refresh = binding.refresh,
        .sequence = sequence,
    });
    errdefer alloc.free(value);
    const okey = outboxKey(sequence, result.next_ordinal);
    try plan.putOwned(try alloc.dupe(u8, &okey), value);
    result.next_ordinal += 1;
    result.notices += 1;
}

pub const EdgeChange = struct {
    graph_index: []const u8,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    /// Generated (artifact-derived) edges never reach live graph context.
    artifact_derived: bool = false,
};

/// Stage adjacency notices for direct graph edge writes/deletes: the
/// traversal through an endpoint can change, so the endpoint's graph tuple
/// is notified in the traversal orientations each spec declares.
pub fn planStageEdgeNotices(alloc: Allocator, plan: *store.Plan, specs: *const SourceSpecs, own_table_id: []const u8, sequence: u64, first_ordinal: u32, edges: []const EdgeChange) !StageResult {
    var result = StageResult{ .next_ordinal = first_ordinal };
    for (edges) |edge| {
        for (specs.items) |binding| {
            const g = binding.source.graph;
            if (binding.source.kind != .graph or !std.mem.eql(u8, g.graph_index, edge.graph_index)) continue;
            if (edge.artifact_derived and !g.include_generated) continue;
            if (g.edge_types.len != 0) {
                var admitted = false;
                for (g.edge_types) |t| if (std.mem.eql(u8, t, edge.edge_type)) {
                    admitted = true;
                };
                if (!admitted) continue;
            }
            if (g.direction != .in) try stage(alloc, plan, binding, own_table_id, edge.source, ctx.graphTuple(binding.spec, edge.source), false, .edge_changed, sequence, &result);
            if (g.direction != .out) try stage(alloc, plan, binding, own_table_id, edge.target, ctx.graphTuple(binding.spec, edge.target), false, .edge_changed, sequence, &result);
            // A depth>1 traversal also reads through the far endpoint.
            if (g.depth > 1) {
                const far = if (g.direction == .in) edge.source else edge.target;
                try stage(alloc, plan, binding, own_table_id, far, ctx.graphTuple(binding.spec, far), false, .edge_changed, sequence, &result);
            }
        }
    }
    return result;
}

pub const BackfillResult = struct {
    rows: usize,
    resume_after: ?[]u8,
};

/// Write covering shadows for existing rows in one bounded page. Callers
/// pass rows from a document scan (key + logical JSON); the scan cursor is
/// the caller's. Writes nothing for rows whose shadow already covers the
/// current fields.
pub fn planBackfillShadows(alloc: Allocator, reader: store.Reader, plan: *store.Plan, specs: *const SourceSpecs, rows: []const RowChange) !usize {
    var written: usize = 0;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    for (rows) |row| {
        _ = arena.reset(.retain_capacity);
        const a = arena.allocator();
        const raw = row.next orelse continue;
        const skey = try shadowKeyAlloc(a, row.key);
        if (try reader.get(a, skey)) |existing| {
            if (existing.len >= 32 and std.mem.eql(u8, existing[0..32], &specs.fields_digest)) continue;
        }
        const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{}) catch continue;
        if (parsed != .object) continue;
        const projection = try ctx.projectTreeAlloc(a, parsed, specs.union_fields);
        try plan.putOwned(try alloc.dupe(u8, skey), try encodeShadowAlloc(alloc, specs.fields_digest, projection));
        written += 1;
    }
    return written;
}

// ---------------------------------------------------------------------------
// Dependent side
// ---------------------------------------------------------------------------

pub const Target = struct {
    artifact: []u8,
    doc_key: []u8,
};

pub const ApplyResult = struct {
    /// Rebuild now (background, no debounce).
    rebuild: std.ArrayListUnmanaged(Target) = .empty,
    /// Already queued in the debounce queue by the plan.
    debounced: usize = 0,
    /// Marked stale only (manual refresh).
    stale_only: usize = 0,
    /// Remaining fan-out to re-enqueue (owned encoded notice), if any.
    continuation: ?[]u8 = null,

    pub fn deinit(self: *ApplyResult, alloc: Allocator) void {
        for (self.rebuild.items) |t| {
            alloc.free(t.artifact);
            alloc.free(t.doc_key);
        }
        self.rebuild.deinit(alloc);
        if (self.continuation) |c| alloc.free(c);
        self.* = undefined;
    }
};

/// Apply one notice to this dependent DB's subscriptions. `range_*` bound
/// the dependents this shard owns.
pub fn planApplyNotice(
    alloc: Allocator,
    reader: store.Reader,
    plan: *store.Plan,
    notice: Notice,
    now_ms: u64,
    range_lower: []const u8,
    range_upper: []const u8,
    page: usize,
) !ApplyResult {
    return planApplyNoticeWithSources(alloc, reader, plan, notice, now_ms, range_lower, range_upper, page, null);
}

/// Resolves a dependent's source declaration so residual notices can be
/// evaluated against each subscriber's parameters.
pub const SourceLookup = struct {
    ptr: *anyopaque,
    lookup_fn: *const fn (ptr: *anyopaque, artifact: []const u8, spec: Digest) ?*const ctx.Source,
};

const RelevanceContext = struct {
    alloc: Allocator,
    sources: SourceLookup,
    notice: Notice,
    prev: ?std.json.Value,
    next: ?std.json.Value,
    prev_unknown: bool,

    fn relevant(p: *anyopaque, artifact: []const u8, residual: []const u8) anyerror!bool {
        const self: *RelevanceContext = @ptrCast(@alignCast(p));
        if (self.prev_unknown or residual.len == 0) return true;
        const source = self.sources.lookup_fn(self.sources.ptr, artifact, self.notice.spec) orelse return true;
        return ctx.subscriberRelevant(self.alloc, source, residual, self.notice.key, self.prev, self.next);
    }
};

pub fn planApplyNoticeWithSources(
    alloc: Allocator,
    reader: store.Reader,
    plan: *store.Plan,
    notice: Notice,
    now_ms: u64,
    range_lower: []const u8,
    range_upper: []const u8,
    page: usize,
    sources: ?SourceLookup,
) !ApplyResult {
    var rows_arena = std.heap.ArenaAllocator.init(alloc);
    defer rows_arena.deinit();
    var relevance_ctx: RelevanceContext = undefined;
    var relevance: ?store.Relevance = null;
    if (sources) |lookup| if (notice.prev_row.len != 0 or notice.next_row.len != 0) {
        const ra = rows_arena.allocator();
        relevance_ctx = .{
            .alloc = alloc,
            .sources = lookup,
            .notice = notice,
            .prev_unknown = std.mem.eql(u8, notice.prev_row, "?"),
            .prev = if (notice.prev_row.len == 0 or std.mem.eql(u8, notice.prev_row, "?")) null else std.json.parseFromSliceLeaky(std.json.Value, ra, notice.prev_row, .{}) catch null,
            .next = if (notice.next_row.len == 0) null else std.json.parseFromSliceLeaky(std.json.Value, ra, notice.next_row, .{}) catch null,
        };
        relevance = .{ .ptr = &relevance_ctx, .relevant_fn = RelevanceContext.relevant };
    };
    var result = ApplyResult{};
    errdefer result.deinit(alloc);
    var pending_continuation: ?[]u8 = null;
    errdefer if (pending_continuation) |c| alloc.free(c);
    const background = notice.refresh.mode == .background;
    const options = store.InvalidateOptions{
        .source = notice.source_name,
        .change = notice.change,
        .table_id = notice.source_table_id,
        .key = notice.key,
        .queue_rebuild = background,
        .range_lower = range_lower,
        .range_upper = range_upper,
        .resume_after = notice.resume_after,
        .max_targets = page,
        .relevance = relevance,
    };
    var invalidated = if (notice.all)
        try store.planInvalidateSpec(alloc, reader, plan, notice.spec, options)
    else
        try store.planInvalidateTuple(alloc, reader, plan, notice.spec, notice.tuple, options);
    defer invalidated.deinit(alloc);
    // Further tuples of the same change, each fully drained before the next
    // unless the page budget is reached (then the continuation carries them).
    var more_index: usize = 0;
    while (invalidated.resume_after == null and more_index < notice.more_tuples.len and invalidated.targets.items.len < page) : (more_index += 1) {
        var next_options = options;
        next_options.resume_after = "";
        next_options.max_targets = page - invalidated.targets.items.len;
        var extra = try store.planInvalidateTuple(alloc, reader, plan, notice.spec, notice.more_tuples[more_index], next_options);
        defer extra.deinit(alloc);
        for (extra.targets.items) |t| {
            var duplicate = false;
            for (invalidated.targets.items) |existing| if (std.mem.eql(u8, existing.artifact, t.artifact) and std.mem.eql(u8, existing.doc_key, t.doc_key)) {
                duplicate = true;
            };
            if (duplicate) continue;
            try invalidated.targets.append(alloc, .{ .artifact = try alloc.dupe(u8, t.artifact), .doc_key = try alloc.dupe(u8, t.doc_key), .newly_stale = t.newly_stale });
        }
        if (extra.resume_after) |cursor| {
            // Continue inside this tuple next time.
            var next = notice;
            next.tuple = notice.more_tuples[more_index];
            next.more_tuples = notice.more_tuples[more_index + 1 ..];
            next.resume_after = cursor;
            pending_continuation = try encodeNoticeAlloc(alloc, next);
            more_index = notice.more_tuples.len;
            break;
        }
    }
    for (invalidated.targets.items) |t| {
        // A spec may be shared by identical declarations on several
        // enrichments; only the notice's artifact is rebuilt by it.
        if (!std.mem.eql(u8, t.artifact, notice.artifact)) continue;
        if (!background) {
            result.stale_only += 1;
        } else if (notice.refresh.debounce_ms != 0) {
            try store.planQueueDebounced(plan, now_ms +| notice.refresh.debounce_ms, t.artifact, t.doc_key);
            result.debounced += 1;
        } else {
            const artifact = try alloc.dupe(u8, t.artifact);
            errdefer alloc.free(artifact);
            try result.rebuild.append(alloc, .{ .artifact = artifact, .doc_key = try alloc.dupe(u8, t.doc_key) });
        }
    }
    if (pending_continuation) |c| {
        result.continuation = c;
    } else if (invalidated.resume_after) |cursor| {
        var next = notice;
        next.resume_after = cursor;
        result.continuation = try encodeNoticeAlloc(alloc, next);
    } else if (more_index < notice.more_tuples.len) {
        var next = notice;
        next.tuple = notice.more_tuples[more_index];
        next.more_tuples = notice.more_tuples[more_index + 1 ..];
        next.resume_after = "";
        result.continuation = try encodeNoticeAlloc(alloc, next);
    }
    return result;
}

pub const OutboxEntry = struct {
    key: []u8,
    value: []u8,
};

pub fn readOutboxAlloc(alloc: Allocator, reader: store.Reader, limit: usize) ![]OutboxEntry {
    var out = std.ArrayListUnmanaged(OutboxEntry).empty;
    errdefer {
        for (out.items) |e| {
            alloc.free(e.key);
            alloc.free(e.value);
        }
        out.deinit(alloc);
    }
    const Collect = struct {
        alloc: Allocator,
        out: *std.ArrayListUnmanaged(OutboxEntry),
        limit: usize,
        fn visit(p: *anyopaque, key: []const u8, value: []const u8) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(p));
            const k = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(k);
            try self.out.append(self.alloc, .{ .key = k, .value = try self.alloc.dupe(u8, value) });
            return self.out.items.len < self.limit;
        }
    };
    var collect = Collect{ .alloc = alloc, .out = &out, .limit = limit };
    try reader.scanPrefix(alloc, &store.familyPrefix(outbox_family), .{ .ptr = &collect, .visit_fn = Collect.visit });
    return try out.toOwnedSlice(alloc);
}

pub fn freeOutbox(alloc: Allocator, entries: []OutboxEntry) void {
    for (entries) |e| {
        alloc.free(e.key);
        alloc.free(e.value);
    }
    alloc.free(entries);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const policies_config =
    \\{"sources":{"policies":{"query":{"table":"policies","table_id":"tp","filter_query":{"conjuncts":[
    \\ {"field":"org","term":{"$source":"/org"}},{"field":"status","term":"approved"}]}},
    \\ "fields":["text"],"max_documents":10,"max_bytes":4096,"overflow":"error"},
    \\ "org":{"document":{"table":"orgs","table_id":"to","key_from":"/org"},"fields":["voice"],"required":false,"max_bytes":4096,"overflow":"error"}},
    \\ "refresh":{"mode":"background","debounce_ms":0}}
;

fn stageOne(alloc: Allocator, s: *store.MemoryStore, specs: *const SourceSpecs, table: []const u8, seq: u64, key: []const u8, next: ?[]const u8) !usize {
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    const r = try planStageNotices(alloc, s.reader(), &plan, specs, table, seq, 0, &.{.{ .key = key, .next = next }}, true);
    try s.apply(&plan);
    return r.notices;
}

test "source specs select only sources targeting this table" {
    const alloc = testing.allocator;
    var specs = try buildSourceSpecs(alloc, "tp", &.{.{ .dependent_table_id = "td", .artifact = "summary", .stored_json = policies_config }});
    defer specs.deinit();
    try testing.expectEqual(@as(usize, 1), specs.items.len);
    try testing.expectEqualStrings("policies", specs.items[0].source.name);
    try testing.expectEqual(@as(usize, 3), specs.union_fields.len); // org, status, text
    var none = try buildSourceSpecs(alloc, "other", &.{.{ .dependent_table_id = "td", .artifact = "summary", .stored_json = policies_config }});
    defer none.deinit();
    try testing.expect(none.isEmpty());
}

test "end to end: staged notices invalidate exactly the affected dependents" {
    const alloc = testing.allocator;
    var source_store = store.MemoryStore{ .alloc = alloc };
    defer source_store.deinit();
    var dependent_store = store.MemoryStore{ .alloc = alloc };
    defer dependent_store.deinit();
    var specs = try buildSourceSpecs(alloc, "tp", &.{.{ .dependent_table_id = "td", .artifact = "summary", .stored_json = policies_config }});
    defer specs.deinit();

    // Dependents: doc-a binds org o1, doc-b binds org o2.
    var config = try ctx.parseStoredJson(alloc, policies_config);
    defer config.deinit();
    for ([_][2][]const u8{ .{ "doc-a", "{\"org\":\"o1\"}" }, .{ "doc-b", "{\"org\":\"o2\"}" } }) |dep| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const doc = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), dep[1], .{});
        var bindings = try ctx.bindAlloc(alloc, &config, doc);
        defer bindings.deinit();
        const tuples = try bindings.allTuplesAlloc(alloc);
        defer alloc.free(tuples);
        var plan = store.Plan.init(alloc);
        defer plan.deinit();
        const c = try store.planRegister(alloc, dependent_store.reader(), &plan, dep[0], "summary", tuples, config.digest());
        try dependent_store.apply(&plan);
        plan.deinit();
        plan = store.Plan.init(alloc);
        _ = try store.planPublish(alloc, dependent_store.reader(), &plan, dep[0], "summary", .{ .capture = c, .tuples = tuples, .input_fingerprint = @splat(1) });
        try dependent_store.apply(&plan);
    }

    const Deliver = struct {
        fn run(a: Allocator, src: *store.MemoryStore, dst: *store.MemoryStore) ![]const u8 {
            const entries = try readOutboxAlloc(a, src.reader(), 100);
            defer freeOutbox(a, entries);
            var rebuilt = std.ArrayListUnmanaged(u8).empty;
            defer rebuilt.deinit(a);
            var plan = store.Plan.init(a);
            defer plan.deinit();
            for (entries) |e| {
                var decoded = try decodeNoticeAlloc(a, e.value);
                defer decoded.deinit();
                var applied = try planApplyNotice(a, dst.reader(), &plan, decoded.notice, 0, "", "", 16);
                defer applied.deinit(a);
                for (applied.rebuild.items) |t| {
                    try rebuilt.appendSlice(a, t.doc_key);
                    try rebuilt.append(a, ' ');
                }
                try plan.delete(e.key);
            }
            try dst.apply(&plan);
            try src.apply(&plan);
            return try a.dupe(u8, rebuilt.items);
        }
    };

    // First approved policy for o1 (empty result before): doc-a only.
    try testing.expectEqual(@as(usize, 1), try stageOne(alloc, &source_store, &specs, "tp", 1, "p1", "{\"org\":\"o1\",\"status\":\"approved\",\"text\":\"t\"}"));
    const r1 = try Deliver.run(alloc, &source_store, &dependent_store);
    defer alloc.free(r1);
    try testing.expectEqualStrings("doc-a ", r1);

    // Unconsumed field edit: nothing staged.
    try testing.expectEqual(@as(usize, 0), try stageOne(alloc, &source_store, &specs, "tp", 2, "p1", "{\"org\":\"o1\",\"status\":\"approved\",\"text\":\"t\",\"note\":\"x\"}"));
    // Draft in another org: nothing.
    try testing.expectEqual(@as(usize, 0), try stageOne(alloc, &source_store, &specs, "tp", 3, "p9", "{\"org\":\"o2\",\"status\":\"draft\",\"text\":\"t\"}"));
    // Move p1 from o1 to o2: leaves o1 and enters o2 -> both dependents.
    // One notice carries both touched tuples (leaving o1, entering o2).
    try testing.expectEqual(@as(usize, 1), try stageOne(alloc, &source_store, &specs, "tp", 4, "p1", "{\"org\":\"o2\",\"status\":\"approved\",\"text\":\"t\"}"));
    const r2 = try Deliver.run(alloc, &source_store, &dependent_store);
    defer alloc.free(r2);
    try testing.expect(std.mem.indexOf(u8, r2, "doc-a") != null and std.mem.indexOf(u8, r2, "doc-b") != null);
    // Delete (or TTL expiry): leaves o2.
    try testing.expectEqual(@as(usize, 1), try stageOne(alloc, &source_store, &specs, "tp", 5, "p1", null));
    const r3 = try Deliver.run(alloc, &source_store, &dependent_store);
    defer alloc.free(r3);
    try testing.expectEqualStrings("doc-b ", r3);
    try testing.expectEqual(@as(usize, 0), source_store.countPrefix(&store.familyPrefix(outbox_family)));

    var state = (try store.loadState(alloc, dependent_store.reader(), null, "doc-b", "summary")).?;
    defer state.deinit(alloc);
    try testing.expectEqual(store.Freshness.stale, state.freshness);
    try testing.expectEqual(@as(usize, 2), state.reasons.len);
}

test "rows without covering shadows notify spec-wide until backfilled" {
    const alloc = testing.allocator;
    var s = store.MemoryStore{ .alloc = alloc };
    defer s.deinit();
    var specs = try buildSourceSpecs(alloc, "tp", &.{.{ .dependent_table_id = "td", .artifact = "summary", .stored_json = policies_config }});
    defer specs.deinit();
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    const r = try planStageNotices(alloc, s.reader(), &plan, &specs, "tp", 1, 0, &.{.{ .key = "old", .next = "{\"org\":\"o1\",\"status\":\"approved\"}" }}, false);
    try testing.expectEqual(@as(usize, 1), r.notices);
    const entry = plan.puts.items[0];
    var decoded = try decodeNoticeAlloc(alloc, entry.value);
    defer decoded.deinit();
    try testing.expect(decoded.notice.all);

    var bplan = store.Plan.init(alloc);
    defer bplan.deinit();
    try testing.expectEqual(@as(usize, 1), try planBackfillShadows(alloc, s.reader(), &bplan, &specs, &.{.{ .key = "older", .next = "{\"org\":\"o2\",\"status\":\"approved\"}" }}));
    try s.apply(&bplan);
    var plan2 = store.Plan.init(alloc);
    defer plan2.deinit();
    const r2 = try planStageNotices(alloc, s.reader(), &plan2, &specs, "tp", 2, 0, &.{.{ .key = "older", .next = "{\"org\":\"o2\",\"status\":\"approved\",\"z\":1}" }}, false);
    try testing.expectEqual(@as(usize, 0), r2.notices);
}

test "fan-out continuation, manual refresh, and debounce" {
    const alloc = testing.allocator;
    var s = store.MemoryStore{ .alloc = alloc };
    defer s.deinit();
    const spec: Digest = @splat(3);
    const tuple: Digest = @splat(4);
    var buf: [16]u8 = undefined;
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    for (0..5) |i| _ = try store.planRegister(alloc, s.reader(), &plan, try std.fmt.bufPrint(&buf, "d{d}", .{i}), "x", &.{.{ .spec = spec, .tuple = tuple }}, @splat(1));
    try s.apply(&plan);
    const base = Notice{ .dependent_table_id = "", .artifact = "x", .source_name = "s", .source_table_id = "t", .key = "k", .spec = spec, .tuple = tuple, .change = .member_added, .refresh = .{}, .sequence = 1 };
    var total: usize = 0;
    var current: ?DecodedNotice = null;
    defer if (current) |*c| c.deinit();
    var pages: usize = 0;
    while (true) : (pages += 1) {
        var p = store.Plan.init(alloc);
        defer p.deinit();
        var applied = try planApplyNotice(alloc, s.reader(), &p, if (current) |c| c.notice else base, 0, "", "", 2);
        defer applied.deinit(alloc);
        total += applied.rebuild.items.len;
        try s.apply(&p);
        const cont = applied.continuation orelse break;
        const next = try decodeNoticeAlloc(alloc, cont);
        if (current) |*c| c.deinit();
        current = next;
    }
    try testing.expectEqual(@as(usize, 2), pages);
    try testing.expectEqual(@as(usize, 5), total);

    var manual = base;
    manual.refresh = .{ .mode = .manual };
    var mp = store.Plan.init(alloc);
    defer mp.deinit();
    var applied = try planApplyNotice(alloc, s.reader(), &mp, manual, 0, "", "", 16);
    defer applied.deinit(alloc);
    try testing.expectEqual(@as(usize, 5), applied.stale_only);
    try testing.expectEqual(@as(usize, 0), applied.rebuild.items.len);

    var debounced = manual;
    debounced.refresh = .{ .mode = .background, .debounce_ms = 2000 };
    var dp = store.Plan.init(alloc);
    defer dp.deinit();
    var da = try planApplyNotice(alloc, s.reader(), &dp, debounced, 1000, "", "", 16);
    defer da.deinit(alloc);
    try testing.expectEqual(@as(usize, 5), da.debounced);
    try s.apply(&dp);
    var drain = store.Plan.init(alloc);
    defer drain.deinit();
    const early = try store.planDrainDebounced(alloc, s.reader(), &drain, 2000, 100);
    defer store.freeTargets(alloc, early);
    try testing.expectEqual(@as(usize, 0), early.len);
    const due = try store.planDrainDebounced(alloc, s.reader(), &drain, 3000, 100);
    defer store.freeTargets(alloc, due);
    try testing.expectEqual(@as(usize, 5), due.len);
}

test "notice encoding round trips" {
    const alloc = testing.allocator;
    const raw = try encodeNoticeAlloc(alloc, .{ .dependent_table_id = "d\"q", .artifact = "a", .source_name = "s", .source_table_id = "t", .key = "k\x00", .spec = @splat(1), .tuple = @splat(2), .change = .member_changed, .refresh = .{ .mode = .manual, .debounce_ms = 5 }, .sequence = 9, .resume_after = "\x01\x02" });
    defer alloc.free(raw);
    var decoded = try decodeNoticeAlloc(alloc, raw);
    defer decoded.deinit();
    try testing.expectEqualStrings("d\"q", decoded.notice.dependent_table_id);
    try testing.expectEqualStrings("k\x00", decoded.notice.key);
    try testing.expectEqual(ctx.RefreshMode.manual, decoded.notice.refresh.mode);
    try testing.expectEqualSlices(u8, "\x01\x02", decoded.notice.resume_after);
}

test "derived graph edges notify only sources that include generated edges" {
    const alloc = testing.allocator;
    const config =
        \\{"sources":{"direct":{"graph":{"graph_index":"kg","edge_types":[],"direction":"out","depth":1,"max_fanout":10},"fields":["t"],"max_documents":10,"max_bytes":4096,"overflow":"error"},
        \\ "derived":{"graph":{"graph_index":"kg","edge_types":[],"direction":"out","depth":1,"max_fanout":10,"include_generated":true},"fields":["t"],"max_documents":10,"max_bytes":4096,"overflow":"error"}},
        \\ "refresh":{"mode":"background","debounce_ms":0}}
    ;
    var specs = try buildSourceSpecs(alloc, "t", &.{.{ .dependent_table_id = "", .artifact = "summary", .stored_json = config }});
    defer specs.deinit();
    try testing.expect(specs.generated_edges);
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    const generated = try planStageEdgeNotices(alloc, &plan, &specs, "t", 1, 0, &.{.{ .graph_index = "kg", .source = "a", .target = "b", .edge_type = "rel", .artifact_derived = true }});
    try testing.expectEqual(@as(usize, 1), generated.notices);
    const direct = try planStageEdgeNotices(alloc, &plan, &specs, "t", 2, 0, &.{.{ .graph_index = "kg", .source = "a", .target = "b", .edge_type = "rel" }});
    try testing.expectEqual(@as(usize, 2), direct.notices);
}

test "range-aware notices carry every touched block and page across them" {
    const alloc = testing.allocator;
    var s = store.MemoryStore{ .alloc = alloc };
    defer s.deinit();
    const config =
        \\{"sources":{"w":{"query":{"filter_query":{"conjuncts":[{"field":"at","range":{"gte":{"$source":"/lo"}}}]}},"fields":["t"],"max_documents":100,"max_candidates":1000,"max_bytes":4096,"overflow":"error"}},
        \\ "refresh":{"mode":"background","debounce_ms":0}}
    ;
    var parsed = try ctx.parseStoredJson(alloc, config);
    defer parsed.deinit();
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    var buf: [16]u8 = undefined;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    // Dependents with thresholds 0, 10, ..., 90.
    for (0..10) |i| {
        const doc = try ctx.bindAlloc(alloc, &parsed, try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), try std.fmt.allocPrint(arena.allocator(), "{{\"lo\":{d}}}", .{i * 10}), .{}));
        var bindings = doc;
        defer bindings.deinit();
        const tuples = try bindings.allTuplesAlloc(alloc);
        defer alloc.free(tuples);
        _ = try store.planRegister(alloc, s.reader(), &plan, try std.fmt.bufPrint(&buf, "d{d}", .{i}), "x", tuples, parsed.digest());
    }
    try s.apply(&plan);
    var specs = try buildSourceSpecs(alloc, "t", &.{.{ .dependent_table_id = "", .artifact = "x", .stored_json = config }});
    defer specs.deinit();
    var stage_plan = store.Plan.init(alloc);
    defer stage_plan.deinit();
    const staged = try planStageNotices(alloc, s.reader(), &stage_plan, &specs, "t", 1, 0, &.{.{ .key = "row", .next = "{\"at\":35,\"t\":\"x\"}" }}, true);
    try testing.expectEqual(@as(usize, 1), staged.notices);
    var decoded = try decodeNoticeAlloc(alloc, stage_plan.puts.items[0].value);
    defer decoded.deinit();
    try testing.expect(decoded.notice.more_tuples.len > 0);
    // Dependents with lo <= 35 (d0..d3) are reached through the value's
    // ancestor blocks; small pages continue across tuples.
    var current: ?DecodedNotice = null;
    defer if (current) |*c| c.deinit();
    var reached = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = reached.keyIterator();
        while (it.next()) |k| alloc.free(k.*);
        reached.deinit(alloc);
    }
    var rounds: usize = 0;
    while (true) : (rounds += 1) {
        try testing.expect(rounds < 64);
        var p = store.Plan.init(alloc);
        defer p.deinit();
        var applied = try planApplyNotice(alloc, s.reader(), &p, if (current) |c| c.notice else decoded.notice, 0, "", "", 2);
        defer applied.deinit(alloc);
        for (applied.rebuild.items) |t| if (!reached.contains(t.doc_key)) try reached.put(alloc, try alloc.dupe(u8, t.doc_key), {});
        try s.apply(&p);
        const cont = applied.continuation orelse break;
        const next = try decodeNoticeAlloc(alloc, cont);
        if (current) |*c| c.deinit();
        current = next;
    }
    // Block matching over-approximates; per-subscriber residual filtering
    // (applied by the DB with source lookup) narrows further. Every true
    // dependent must be reached.
    for ([_][]const u8{ "d0", "d1", "d2", "d3" }) |d| try testing.expect(reached.contains(d));
    try testing.expect(!reached.contains("d9"));
}
