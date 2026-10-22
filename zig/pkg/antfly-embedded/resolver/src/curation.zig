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

//! Indexed, execution-local view of durable resolver curation. Strings and maps
//! belong to the caller's arena; construction is linear in policy size, while
//! mention and candidate admission require constant-time lookups.
const std = @import("std");
const slug = @import("unicode_slug.zig");

pub const Origin = struct {
    origin: enum { manual, adjudicator, review, cascade, config } = .manual,
    actor: []const u8 = "",
    reason: []const u8 = "",
    created_at_ms: u64 = 0,
    revision: u64 = 0,
    proposal_id: ?[]const u8 = null,
    cascade_from: ?[]const u8 = null,

    pub fn validate(self: Origin) !void {
        if (self.proposal_id) |id| {
            if (id.len != 64) return error.InvalidResolverCurationRequest;
            for (id) |byte| if (!std.ascii.isHex(byte)) return error.InvalidResolverCurationRequest;
        }
        if (self.cascade_from) |key| try validKey(key);
        if (self.actor.len > 256 or self.reason.len > 4096 or !std.unicode.utf8ValidateSlice(self.actor) or !std.unicode.utf8ValidateSlice(self.reason)) return error.InvalidResolverCurationRequest;
    }
};
pub const Entry = struct { key: []const u8, origin: Origin };
pub const RedirectOrigin = struct { key: []const u8, target: []const u8, origin: Origin };
pub const Absorbed = enum { reject, release, suppress };
pub const AbsorbedResult = struct { key: []const u8, disposition: Absorbed };
pub const Suppression = struct {
    keys: []const []const u8 = &.{},
    texts: []const []const u8 = &.{},
    entries: []const Entry = &.{},
    text_entries: []const Entry = &.{},
    min_text_chars: u32 = 0,
};
pub const Pair = struct { from: []const u8, to: []const u8, origin: ?Origin = null };
/// Durable identity of one queue lifetime, independent of its table name.
/// A queue receives its identity before its first proposal is persisted.
pub const ReceiptScope = [32]u8;
pub fn sameReceiptScope(left: ?ReceiptScope, right: ?ReceiptScope) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, &left.?, &right.?);
}
pub fn bindReceiptScope(state: State, scope: ?ReceiptScope) State {
    if (sameReceiptScope(state.receipt_scope, scope)) return state;
    var next = state;
    next.receipt_scope = scope;
    next.proposals = &.{};
    return next;
}
pub const State = struct {
    version: u32 = 1,
    suppress: Suppression = .{},
    distinct: []const Pair = &.{},
    redirect_origins: []const RedirectOrigin = &.{},
    proposals: []const Proposal = &.{},
    receipt_scope: ?ReceiptScope = null,
    /// Mask a declarative alias after an explicit unmerge. Future graph
    /// replacements must not silently recreate the redirect just corrected.
    removed_aliases: []const []const u8 = &.{},
};
pub const ProposalState = enum { open, accepted, rejected, superseded };
pub const EntityAction = enum { merge, unmerge, suppress, unsuppress, distinct };
pub const Options = struct { replace: bool = false, pin_distinct: bool = false, absorbed: Absorbed = .reject };
pub const Proposal = struct {
    id: []const u8,
    action: EntityAction,
    key: []const u8,
    into: ?[]const u8 = null,
    options: Options = .{},
    confidence: ?f64 = null,
    evidence: ?std.json.Value = null,
    origin: Origin,
    state: ProposalState = .open,
    decided: ?Origin = null,
};
pub const ProposalRequest = struct {
    action: EntityAction,
    key: []const u8,
    into: ?[]const u8 = null,
    options: Options = .{},
    confidence: ?f64 = null,
    evidence: ?std.json.Value = null,
    actor: []const u8 = "",
    reason: []const u8 = "",
};
pub const ProposalDecision = struct {
    accept: bool,
    options: ?Options = null,
    pin_distinct: bool = false,
    actor: []const u8 = "",
    reason: []const u8 = "",
    expected_revision: ?u64 = null,
    wait: ?Wait = null,
};
pub fn proposalId(a: std.mem.Allocator, input: ProposalRequest) ![]const u8 {
    try validKey(input.key);
    if (input.into) |into| try validKey(into);
    if ((input.action == .merge or input.action == .distinct) != (input.into != null)) return error.InvalidResolverCurationRequest;
    if (input.into) |into| if (std.mem.eql(u8, input.key, into)) return error.InvalidResolverCurationRequest;
    const swapped = input.action == .distinct and std.mem.order(u8, input.key, input.into.?) == .gt;
    const bytes = try std.json.Stringify.valueAlloc(a, .{ input.action, if (swapped) input.into.? else input.key, if (swapped) input.key else input.into }, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
fn validateEvidence(a: std.mem.Allocator, evidence: std.json.Value) !void {
    if (evidence != .object) return error.InvalidResolverCurationRequest;
    // Bounded depth before using the ordinary JSON writer.
    const Node = struct { value: std.json.Value, depth: usize };
    var stack: std.ArrayListUnmanaged(Node) = .empty;
    try stack.append(a, .{ .value = evidence, .depth = 0 });
    var nodes: usize = 0;
    while (stack.pop()) |node| {
        nodes += 1;
        if (node.depth > 32 or nodes > 4096) return error.ResolverCurationLimitExceeded;
        switch (node.value) {
            .array => |items| {
                if (items.items.len + stack.items.len > 4096) return error.ResolverCurationLimitExceeded;
                for (items.items) |value| {
                    try stack.append(a, .{ .value = value, .depth = node.depth + 1 });
                }
            },
            .object => |object| {
                if (object.count() + stack.items.len > 4096) return error.ResolverCurationLimitExceeded;
                for (object.values()) |value| {
                    try stack.append(a, .{ .value = value, .depth = node.depth + 1 });
                }
            },
            else => {},
        }
    }
    if (jsonSize(evidence) > 16384) return error.ResolverCurationLimitExceeded;
}
pub fn propose(a: std.mem.Allocator, state: State, merges: []const Redirect, input: ProposalRequest, stamp: Origin) !Plan {
    try validateProposalOrigin(stamp);
    const id = try proposalId(a, input);
    if (input.options.absorbed != .reject and input.action != .suppress) return error.InvalidResolverCurationRequest;
    if (input.options.replace and input.action != .merge) return error.InvalidResolverCurationRequest;
    if (input.options.pin_distinct and input.action != .merge and input.action != .unmerge and !(input.action == .suppress and input.options.absorbed == .release)) return error.InvalidResolverCurationRequest;
    for (state.proposals) |proposal| if (std.mem.eql(u8, proposal.id, id)) return .{ .state = state, .merges = merges, .changed = false };
    if (state.proposals.len >= 4096) return error.ResolverCurationLimitExceeded;
    if (input.confidence) |confidence| if (!std.math.isFinite(confidence) or confidence < 0 or confidence > 1) return error.InvalidResolverCurationRequest;
    if (input.evidence) |evidence| try validateEvidence(a, evidence);
    const entries = try a.alloc(Proposal, state.proposals.len + 1);
    @memcpy(entries[0..state.proposals.len], state.proposals);
    entries[state.proposals.len] = .{ .id = id, .action = input.action, .key = input.key, .into = input.into, .options = input.options, .confidence = input.confidence, .evidence = input.evidence, .origin = stamp };
    var next = state;
    next.proposals = entries;
    try validateProposalQueue(next);
    return .{ .state = next, .merges = merges, .changed = true };
}
/// Replace the inputs/options of an open queue item. Callers retain immutable
/// attempt history and fence review against the new revision; terminal queue
/// outcomes can never be reset through this operation.
pub fn refreshProposal(a: std.mem.Allocator, state: State, merges: []const Redirect, input: ProposalRequest, stamp: Origin) !Plan {
    const candidate = try propose(a, .{}, merges, input, stamp);
    const replacement = candidate.state.proposals[0];
    const selected = for (state.proposals, 0..) |proposal, i| {
        if (std.mem.eql(u8, proposal.id, replacement.id)) break i;
    } else return error.NotFound;
    const previous = state.proposals[selected];
    if (previous.state != .open or stamp.revision <= previous.origin.revision) return error.ResolverCurationConflict;
    const entries = try a.dupe(Proposal, state.proposals);
    entries[selected] = replacement;
    var next = state;
    next.proposals = entries;
    try validateProposalQueue(next);
    return .{ .state = next, .merges = merges, .changed = true };
}
pub fn decide(a: std.mem.Allocator, state: State, merges: []const Redirect, effective: []const Redirect, id: []const u8, input: ProposalDecision, stamp: Origin) !Plan {
    try validateProposalOrigin(stamp);
    const selected = for (state.proposals, 0..) |proposal, i| {
        if (std.mem.eql(u8, proposal.id, id)) break i;
    } else return error.NotFound;
    const proposal = state.proposals[selected];
    if (!input.accept and input.options != null) return error.InvalidResolverCurationRequest;
    if (input.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
    const wanted: ProposalState = if (input.accept) .accepted else .rejected;
    if (proposal.state == wanted) return .{ .state = state, .merges = merges, .changed = false };
    if (proposal.state != .open) return error.ResolverCurationConflict;
    if (input.pin_distinct and (input.accept or proposal.action != .merge)) return error.InvalidResolverCurationRequest;
    var origin = stamp;
    origin.origin = .review;
    origin.proposal_id = proposal.id;
    var result: Plan = .{ .state = state, .merges = merges, .changed = true };
    if (input.accept or input.pin_distinct) {
        const options = if (!input.accept) Options{} else input.options orelse proposal.options;
        result = try planWithOrigin(a, try std.json.Stringify.valueAlloc(a, state, .{}), merges, effective, .{
            .action = if (!input.accept) .distinct else switch (proposal.action) {
                .merge => .merge,
                .unmerge => .unmerge,
                .suppress => .suppress,
                .unsuppress => .unsuppress,
                .distinct => .distinct,
            },
            .key = proposal.key,
            .to = proposal.into,
            .replace = options.replace,
            .pin_distinct = options.pin_distinct,
            .absorbed = options.absorbed,
        }, origin);
    }
    const proposals = try a.dupe(Proposal, result.state.proposals);
    proposals[selected].state = wanted;
    proposals[selected].decided = origin;
    result.state.proposals = proposals;
    result.changed = true;
    try boundedState(a, result.state);
    return result;
}
pub const ProposalPage = struct { proposals: []const Proposal, next_cursor: ?[]const u8 = null };
/// Stable keyset pagination; decision changes cannot move an item across the
/// cursor. Queue size and evidence are bounded independently of page size.
pub fn proposalPage(a: std.mem.Allocator, state: State, query: []const u8) !ProposalPage {
    var wanted: ?ProposalState = .open;
    var action: ?EntityAction = null;
    var cursor: []const u8 = "";
    var limit: usize = 64;
    var parts = std.mem.splitScalar(u8, std.mem.trimStart(u8, query, "?"), '&');
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    while (parts.next()) |part| {
        if (part.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, part, '=') orelse return error.InvalidResolverCurationRequest;
        const name = part[0..eq];
        const value = part[eq + 1 ..];
        if ((try seen.getOrPut(a, name)).found_existing) return error.InvalidResolverCurationRequest;
        if (std.mem.eql(u8, name, "state")) {
            wanted = if (std.mem.eql(u8, value, "all")) null else std.meta.stringToEnum(ProposalState, value) orelse return error.InvalidResolverCurationRequest;
        } else if (std.mem.eql(u8, name, "action")) {
            action = std.meta.stringToEnum(EntityAction, value) orelse return error.InvalidResolverCurationRequest;
        } else if (std.mem.eql(u8, name, "cursor")) {
            cursor = value;
        } else if (std.mem.eql(u8, name, "limit")) {
            limit = try std.fmt.parseInt(usize, value, 10);
        } else return error.InvalidResolverCurationRequest;
    }
    if (limit == 0 or limit > 256) return error.InvalidResolverCurationRequest;
    if (cursor.len > 0) {
        if (cursor.len != 64) return error.InvalidResolverCurationRequest;
        for (cursor) |ch| if (!std.ascii.isHex(ch)) return error.InvalidResolverCurationRequest;
    }
    var items: std.ArrayListUnmanaged(Proposal) = .empty;
    for (state.proposals) |proposal| {
        if (wanted) |value| if (proposal.state != value) continue;
        if (action) |value| if (proposal.action != value) continue;
        if (std.mem.order(u8, proposal.id, cursor) != .gt) continue;
        try items.append(a, proposal);
    }
    std.mem.sort(Proposal, items.items, {}, struct {
        fn less(_: void, x: Proposal, y: Proposal) bool {
            return std.mem.order(u8, x.id, y.id) == .lt;
        }
    }.less);
    const count = @min(limit, items.items.len);
    return .{ .proposals = items.items[0..count], .next_cursor = if (count < items.items.len) items.items[count - 1].id else null };
}

pub const max_state_bytes = 4 * 1024 * 1024;
/// Count encoded bytes with fixed scratch space; do not allocate another copy
/// of a multi-megabyte queue merely to enforce its budget.
pub fn jsonSize(value: anytype) u64 {
    var buffer: [1024]u8 = undefined;
    var sink = std.Io.Writer.Discarding.init(&buffer);
    std.json.Stringify.value(value, .{}, &sink.writer) catch unreachable;
    return sink.fullCount();
}
fn boundedState(_: std.mem.Allocator, state: State) !void {
    if (jsonSize(state) > max_state_bytes) return error.ResolverCurationLimitExceeded;
}
// UTF-8 actor/reason bytes may all require six-byte JSON escapes. The fixed
// allowance covers field names, enum tags, two maximal u64s and a proposal ID.
// Queue decisions have no cascade_from (cascade attribution belongs to policy).
pub const decision_origin_byte_budget = (256 + 4096) * 6 + 512;
fn validateProposalOrigin(origin: Origin) !void {
    try origin.validate();
    if (origin.cascade_from != null) return error.InvalidResolverCurationRequest;
}
pub fn proposalCapacityBytes(state: State) u64 {
    var bytes = jsonSize(state);
    for (state.proposals) |proposal| if (proposal.state == .open) {
        // Replace null with maximal provenance, and open with superseded.
        bytes += decision_origin_byte_budget - 4 + ("superseded".len - "open".len);
    };
    return bytes;
}
pub fn validateProposalQueue(state: State) !void {
    if (state.proposals.len > 4096 or proposalCapacityBytes(state) > max_state_bytes) return error.ResolverCurationLimitExceeded;
}

// The pending decision freezes attribution in both its request and origin.
// Its bound is independent of queue evidence and allows maximal JSON escaping.
pub const max_manifest_bytes = max_state_bytes + 2 * decision_origin_byte_budget + 4096;
pub const proposal_store_key = "\x00\x00__metadata__:curation_proposals";
pub const ProposalManifest = struct {
    queue: State = .{},
    refresh_required: bool = false,
    pending_id: ?[]const u8 = null,
    pending_decision: ?ProposalDecision = null,
    pending_origin: ?Origin = null,
};
pub fn parseProposalManifest(a: std.mem.Allocator, bytes: []const u8) !ProposalManifest {
    if (bytes.len > max_manifest_bytes) return error.ResolverCurationLimitExceeded;
    const manifest = try std.json.parseFromSliceLeaky(ProposalManifest, a, bytes, .{});
    _ = try parseState(a, try std.json.Stringify.valueAlloc(a, manifest.queue, .{}));
    try validateProposalQueue(manifest.queue);
    if (manifest.queue.proposals.len > 0 and manifest.queue.receipt_scope == null) return error.InvalidResolverCurationRequest;
    if ((manifest.pending_id == null) != (manifest.pending_decision == null) or (manifest.pending_id == null) != (manifest.pending_origin == null)) return error.InvalidResolverCurationRequest;
    if (manifest.pending_origin) |origin| try validateProposalOrigin(origin);
    if (manifest.pending_decision) |decision| try validateProposalOrigin(.{ .actor = decision.actor, .reason = decision.reason });
    if (manifest.pending_id) |id| {
        const proposal = for (manifest.queue.proposals) |proposal| {
            if (std.mem.eql(u8, proposal.id, id)) break proposal;
        } else return error.InvalidResolverCurationRequest;
        if (proposal.state != .open) return error.InvalidResolverCurationRequest;
    }
    return manifest;
}

pub const Reason = enum { key, text, min_text_chars, curator };
pub const Redirect = struct { key: []const u8, target: []const u8 };
pub const RedirectIndex = struct {
    canonical_keys: std.StringHashMapUnmanaged([]const u8) = .empty,
    source: []const Redirect,

    pub fn init(a: std.mem.Allocator, aliases: []const Redirect) !RedirectIndex {
        var edges: std.StringHashMapUnmanaged([]const u8) = .empty;
        var out: RedirectIndex = .{ .source = aliases };
        for (aliases) |alias| {
            if (alias.key.len == 0 or alias.target.len == 0) return error.InvalidResolverAlias;
            const entry = try edges.getOrPut(a, alias.key);
            if (entry.found_existing) return error.InvalidResolverAlias;
            entry.value_ptr.* = alias.target;
        }
        var visiting: std.StringHashMapUnmanaged(void) = .empty;
        var path: std.ArrayListUnmanaged([]const u8) = .empty;
        for (aliases) |alias| {
            path.clearRetainingCapacity();
            var key = alias.key;
            const target = walk: while (true) {
                if (out.canonical_keys.get(key)) |known| break :walk known;
                const next = edges.get(key) orelse break :walk key;
                const entry = try visiting.getOrPut(a, key);
                if (entry.found_existing) return error.InvalidResolverAliasCycle;
                try path.append(a, key);
                key = next;
            };
            for (path.items) |visited| {
                try out.canonical_keys.put(a, visited, target);
                _ = visiting.remove(visited);
            }
        }
        return out;
    }
    pub fn canonical(self: RedirectIndex, key: []const u8) []const u8 {
        return self.canonical_keys.get(key) orelse key;
    }
};
/// Entity inspection is a projection of one redirect component, never an
/// alternate endpoint for downloading the source's complete negative policy.
/// Text rules cannot be attributed from an entity key; inspect those through
/// the authorized source resolver suppression resource instead.
pub const EntityState = struct { redirects: []const Redirect, canonical_key: []const u8, curation: State };
pub fn entityState(a: std.mem.Allocator, state: State, aliases: []const Redirect, key: []const u8) !EntityState {
    const index = try RedirectIndex.init(a, aliases);
    const canonical = index.canonical(key);
    var related: std.ArrayListUnmanaged(Redirect) = .empty;
    for (aliases) |alias| if (std.mem.eql(u8, index.canonical(alias.key), canonical)) try related.append(a, alias);
    var cascade_keys: std.StringHashMapUnmanaged(void) = .empty;
    for (state.suppress.entries) |entry| if (entry.origin.cascade_from) |from| {
        if (std.mem.eql(u8, index.canonical(from), canonical)) try cascade_keys.put(a, entry.key, {});
    };
    var keys: std.ArrayListUnmanaged([]const u8) = .empty;
    for (state.suppress.keys) |suppressed| if (std.mem.eql(u8, index.canonical(suppressed), canonical) or cascade_keys.contains(suppressed)) try keys.append(a, suppressed);
    var removed: std.ArrayListUnmanaged([]const u8) = .empty;
    for (state.removed_aliases) |alias| if (std.mem.eql(u8, index.canonical(alias), canonical)) try removed.append(a, alias);
    var pairs: std.ArrayListUnmanaged(Pair) = .empty;
    for (state.distinct) |pair| if (std.mem.eql(u8, index.canonical(pair.from), canonical) or std.mem.eql(u8, index.canonical(pair.to), canonical)) try pairs.append(a, pair);
    var entries: std.ArrayListUnmanaged(Entry) = .empty;
    for (state.suppress.entries) |entry| {
        const from = entry.origin.cascade_from;
        if (std.mem.eql(u8, index.canonical(entry.key), canonical) or (from != null and std.mem.eql(u8, index.canonical(from.?), canonical))) try entries.append(a, entry);
    }
    var origins: std.ArrayListUnmanaged(RedirectOrigin) = .empty;
    for (state.redirect_origins) |entry| if (std.mem.eql(u8, index.canonical(entry.key), canonical)) try origins.append(a, entry);
    return .{ .redirects = related.items, .canonical_key = canonical, .curation = .{
        .version = state.version,
        .suppress = .{ .keys = keys.items, .entries = entries.items, .min_text_chars = state.suppress.min_text_chars },
        .distinct = pairs.items,
        .redirect_origins = origins.items,
        .removed_aliases = removed.items,
    } };
}

pub const Action = enum { merge, unmerge, suppress, unsuppress, suppressions, distinct };
pub const Selection = struct { keys: []const []const u8 = &.{}, texts: []const []const u8 = &.{} };
pub const Wait = struct { timeout_ms: u32 = 1000 };
pub const Request = struct {
    action: Action,
    from: ?[]const u8 = null,
    to: ?[]const u8 = null,
    key: ?[]const u8 = null,
    target: ?[]const u8 = null,
    keys: []const []const u8 = &.{},
    texts: []const []const u8 = &.{},
    actor: []const u8 = "",
    reason: []const u8 = "",
    absorbed: Absorbed = .reject,
    replace: bool = false,
    pin_distinct: bool = false,
    expected_revision: ?u64 = null,
    add: ?Selection = null,
    remove: ?Selection = null,
    wait: ?Wait = null,
};
pub const Plan = struct { state: State, merges: []const Redirect, changed: bool, absorbed: []const AbsorbedResult = &.{} };

/// A bounded catalog edit, built in scratch memory before publication. The
/// catalog validates the complete redirect graph and compares revisions while
/// holding its apply lock. No mutation observes a partially edited policy.
pub fn plan(a: std.mem.Allocator, state_json: []const u8, merges: []const Redirect, effective: []const Redirect, request: Request) !Plan {
    return planWithOrigin(a, state_json, merges, effective, request, .{ .actor = request.actor, .reason = request.reason });
}

/// The admission owner supplies time and revision once. Replay persists this
/// value; clients can supply attribution but cannot forge a server origin.
pub fn planWithOrigin(a: std.mem.Allocator, state_json: []const u8, merges: []const Redirect, effective: []const Redirect, request: Request, origin: Origin) !Plan {
    try origin.validate();
    if (request.absorbed != .reject and request.action != .suppress) return error.InvalidResolverCurationRequest;
    if (request.pin_distinct and request.action == .suppress and request.absorbed != .release) return error.InvalidResolverCurationRequest;
    if (request.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
    if ((request.from != null and request.key != null) or (request.to != null and request.target != null)) return error.InvalidResolverCurationRequest;
    const selections = request.keys.len > 0 or request.texts.len > 0;
    switch (request.action) {
        .merge, .distinct => if (selections or request.add != null or request.remove != null) return error.InvalidResolverCurationRequest,
        .unmerge => if (selections or request.add != null or request.remove != null or request.to != null or request.target != null or request.replace) return error.InvalidResolverCurationRequest,
        .suppress, .unsuppress => if (request.from != null or request.to != null or request.target != null or request.replace or (request.pin_distinct and request.action != .suppress) or request.add != null or request.remove != null) return error.InvalidResolverCurationRequest,
        .suppressions => if (selections or request.key != null or request.from != null or request.to != null or request.target != null or request.replace or request.pin_distinct) return error.InvalidResolverCurationRequest,
    }
    var state = try parseState(a, state_json);
    var next: std.ArrayListUnmanaged(Redirect) = .empty;
    try next.appendSlice(a, merges);
    var changed = false;
    var absorbed: std.ArrayListUnmanaged(AbsorbedResult) = .empty;
    switch (request.action) {
        .suppressions => {
            if (request.add == null and request.remove == null) return error.InvalidResolverCurationRequest;
            if (request.add) |selection| {
                const result = try planWithOrigin(a, try std.json.Stringify.valueAlloc(a, state, .{}), merges, effective, .{ .action = .suppress, .keys = selection.keys, .texts = selection.texts }, origin);
                state = result.state;
            }
            if (request.remove) |selection| {
                const result = try planWithOrigin(a, try std.json.Stringify.valueAlloc(a, state, .{}), merges, effective, .{ .action = .unsuppress, .keys = selection.keys, .texts = selection.texts }, origin);
                state = result.state;
            }
            changed = !std.mem.eql(u8, try std.json.Stringify.valueAlloc(a, try parseState(a, state_json), .{}), try std.json.Stringify.valueAlloc(a, state, .{}));
        },
        .distinct => {
            const from = request.from orelse request.key orelse return error.InvalidResolverCurationRequest;
            const to = request.to orelse request.target orelse return error.InvalidResolverCurationRequest;
            if (request.replace or request.pin_distinct) return error.InvalidResolverCurationRequest;
            changed = try appendPair(a, &state, from, to, origin);
        },
        .merge, .unmerge => {
            const from = request.from orelse request.key orelse return error.InvalidResolverCurationRequest;
            try validKey(from);
            var old: ?[]const u8 = null;
            for (effective) |alias| if (std.mem.eql(u8, alias.key, from)) {
                old = alias.target;
                break;
            };
            if (request.action == .merge) {
                const to = request.to orelse request.target orelse return error.InvalidResolverCurationRequest;
                try validKey(to);
                if (std.mem.eql(u8, from, to)) return error.InvalidResolverAliasCycle;
                if (old) |previous| {
                    if (std.mem.eql(u8, previous, to)) {
                        // A retry of an accepted re-point must remain a no-op,
                        // including when it pinned the former target distinct.
                        return .{ .state = state, .merges = merges, .changed = false };
                    }
                    if (!request.replace) return error.ResolverRedirectExists;
                } else if (request.pin_distinct) return error.InvalidResolverCurationRequest;
                var found = false;
                for (next.items) |*alias| if (std.mem.eql(u8, alias.key, from)) {
                    alias.target = to;
                    found = true;
                    break;
                };
                if (!found) try next.append(a, .{ .key = from, .target = to });
                var records: std.ArrayListUnmanaged(RedirectOrigin) = .empty;
                for (state.redirect_origins) |record| if (!std.mem.eql(u8, record.key, from)) try records.append(a, record);
                try records.append(a, .{ .key = from, .target = to, .origin = origin });
                state.redirect_origins = records.items;
                changed = true;
            } else if (old != null) {
                var i: usize = 0;
                while (i < next.items.len) {
                    if (std.mem.eql(u8, next.items[i].key, from)) {
                        _ = next.orderedRemove(i);
                    } else i += 1;
                }
                state.removed_aliases = try editSet(a, state.removed_aliases, &.{from}, true, false);
                var records: std.ArrayListUnmanaged(RedirectOrigin) = .empty;
                for (state.redirect_origins) |record| if (!std.mem.eql(u8, record.key, from)) try records.append(a, record);
                state.redirect_origins = records.items;
                changed = true;
            }
            if (request.pin_distinct) if (old) |target| {
                const key = try pairKey(a, from, target);
                var present = false;
                for (state.distinct) |pair| if (std.mem.eql(u8, key, try pairKey(a, pair.from, pair.to))) {
                    present = true;
                    break;
                };
                if (!present) {
                    const pairs = try a.alloc(Pair, state.distinct.len + 1);
                    @memcpy(pairs[0..state.distinct.len], state.distinct);
                    pairs[state.distinct.len] = .{ .from = from, .to = target, .origin = origin };
                    state.distinct = pairs;
                    changed = true;
                }
            };
        },
        .suppress, .unsuppress => {
            const add = request.action == .suppress;
            var keys: std.ArrayListUnmanaged([]const u8) = .empty;
            try keys.appendSlice(a, request.keys);
            if (request.key) |key| try keys.append(a, key);
            if (keys.items.len == 0 and request.texts.len == 0) return error.InvalidResolverCurationRequest;
            if (add and keys.items.len > 0) {
                _ = try RedirectIndex.init(a, effective);
                var targets: std.StringHashMapUnmanaged(void) = .empty;
                var roots: std.StringHashMapUnmanaged([]const u8) = .empty;
                var frontier: std.ArrayListUnmanaged([]const u8) = .empty;
                for (keys.items) |key| {
                    if ((try targets.getOrPut(a, key)).found_existing) continue;
                    try roots.put(a, key, key);
                    try frontier.append(a, key);
                }
                var incoming: std.StringHashMapUnmanaged(std.ArrayListUnmanaged([]const u8)) = .empty;
                for (effective) |alias| {
                    const slot = try incoming.getOrPut(a, alias.target);
                    if (!slot.found_existing) slot.value_ptr.* = .empty;
                    try slot.value_ptr.append(a, alias.key);
                }
                var released: std.StringHashMapUnmanaged(void) = .empty;
                var pairs: std.ArrayListUnmanaged(Pair) = .empty;
                var pair_ids: std.StringHashMapUnmanaged(void) = .empty;
                if (request.pin_distinct) {
                    try pairs.appendSlice(a, state.distinct);
                    for (state.distinct) |pair| try pair_ids.put(a, try pairKey(a, pair.from, pair.to), {});
                }
                // Traverse reverse edges once, including targets which are
                // themselves redirected. Compute all effects before editing.
                var position: usize = 0;
                while (position < frontier.items.len) : (position += 1) {
                    const target = frontier.items[position];
                    const root = roots.get(target).?;
                    const sources = incoming.get(target) orelse continue;
                    for (sources.items) |source| {
                        if (request.absorbed == .reject) return error.SuppressedResolverRedirectTarget;
                        if (!(try released.getOrPut(a, source)).found_existing) {
                            try absorbed.append(a, .{ .key = source, .disposition = request.absorbed });
                            if (request.pin_distinct and !(try pair_ids.getOrPut(a, try pairKey(a, source, root))).found_existing) try pairs.append(a, .{ .from = source, .to = root, .origin = origin });
                        }
                        const slot = try roots.getOrPut(a, source);
                        if (!slot.found_existing) {
                            slot.value_ptr.* = root;
                            try frontier.append(a, source);
                        }
                    }
                }
                if (request.pin_distinct) state.distinct = pairs.items;
                if (released.count() > 0) {
                    var kept: std.ArrayListUnmanaged(Redirect) = .empty;
                    for (next.items) |alias| if (!released.contains(alias.key)) try kept.append(a, alias);
                    next = kept;
                    var origins: std.ArrayListUnmanaged(RedirectOrigin) = .empty;
                    for (state.redirect_origins) |entry| if (!released.contains(entry.key)) try origins.append(a, entry);
                    state.redirect_origins = origins.items;
                    state.removed_aliases = try editSet(a, state.removed_aliases, try mapKeys(a, released), true, false);
                    changed = true;
                    if (request.absorbed == .suppress) {
                        var entries: std.StringArrayHashMapUnmanaged(Origin) = .empty;
                        for (state.suppress.entries) |entry| try entries.put(a, entry.key, entry.origin);
                        for (absorbed.items) |item| {
                            if (!targets.contains(item.key)) {
                                const slot = try entries.getOrPut(a, item.key);
                                if (!slot.found_existing) {
                                    var cascade = origin;
                                    cascade.origin = .cascade;
                                    cascade.cascade_from = roots.get(item.key).?;
                                    slot.value_ptr.* = cascade;
                                }
                            }
                            try keys.append(a, item.key);
                        }
                        const attributed = try a.alloc(Entry, entries.count());
                        for (entries.keys(), entries.values(), attributed) |key, value, *entry| entry.* = .{ .key = key, .origin = value };
                        state.suppress.entries = attributed;
                    }
                }
            }
            const before = try std.json.Stringify.valueAlloc(a, state.suppress, .{});
            // Add attribution only on the first assertion; retries retain its
            // original time and revision, including cascade attribution.
            state.suppress.entries = try updateEntries(a, state.suppress.entries, keys.items, add, origin);
            var normalized: std.ArrayListUnmanaged([]const u8) = .empty;
            for (request.texts) |text| try normalized.append(a, try normalize(a, text));
            state.suppress.text_entries = try updateEntries(a, state.suppress.text_entries, normalized.items, add, origin);
            state.suppress.keys = try editSet(a, state.suppress.keys, keys.items, add, false);
            state.suppress.texts = try editSet(a, state.suppress.texts, request.texts, add, true);
            changed = changed or !std.mem.eql(u8, before, try std.json.Stringify.valueAlloc(a, state.suppress, .{}));
        },
    }
    if (changed) state.proposals = try supersede(a, state.proposals, request, absorbed.items, origin);
    try boundedState(a, state);
    return .{ .state = state, .merges = next.items, .changed = changed, .absorbed = absorbed.items };
}
/// Queue supersession is independent of policy serialization. Evidence never
/// enters a resolver catalog or the scratch state used to plan its action.
pub fn supersede(a: std.mem.Allocator, current: []const Proposal, request: Request, absorbed: []const AbsorbedResult, origin: Origin) ![]const Proposal {
    try validateProposalOrigin(origin);
    var affected: std.StringHashMapUnmanaged(void) = .empty;
    const key = request.from orelse request.key;
    if (key) |value| try affected.put(a, value, {});
    for (request.keys) |value| try affected.put(a, value, {});
    if (request.add) |selection| for (selection.keys) |value| try affected.put(a, value, {});
    if (request.remove) |selection| for (selection.keys) |value| try affected.put(a, value, {});
    for (absorbed) |value| try affected.put(a, value.key, {});
    const other = request.to orelse request.target;
    const proposals = try a.dupe(Proposal, current);
    for (proposals) |*proposal| {
        if (proposal.state != .open) continue;
        const references = affected.contains(proposal.key) or (proposal.into != null and affected.contains(proposal.into.?));
        const same_pair = if (key != null and other != null and proposal.into != null)
            (std.mem.eql(u8, key.?, proposal.key) and std.mem.eql(u8, other.?, proposal.into.?)) or
                (std.mem.eql(u8, other.?, proposal.key) and std.mem.eql(u8, key.?, proposal.into.?))
        else
            false;
        const moot = switch (request.action) {
            // Identity changes invalidate older judgments of either endpoint.
            .merge, .unmerge, .suppress, .unsuppress => references,
            // A distinct assertion settles both directions of a merge judgment.
            .distinct => same_pair and (proposal.action == .distinct or proposal.action == .merge),
            .suppressions => references,
        };
        if (moot) {
            proposal.state = .superseded;
            proposal.decided = origin;
        }
    }
    return proposals;
}
/// Persist only terminal supersession receipts with a resolver mutation. The
/// table queue keeps evidence; receipts retain just its stable identity.
pub fn recordSupersessions(a: std.mem.Allocator, edit: *Plan, queue: []const Proposal, request: Request, origin: Origin) ![]const Proposal {
    if (!edit.changed) return queue;
    const projected = try supersede(a, queue, request, edit.absorbed, origin);
    var receipts: std.StringArrayHashMapUnmanaged(Proposal) = .empty;
    for (edit.state.proposals) |receipt| try receipts.put(a, receipt.id, receipt);
    for (queue, projected) |old, proposal| if (old.state == .open and proposal.state == .superseded) {
        var receipt = proposal;
        receipt.evidence = null;
        try receipts.put(a, receipt.id, receipt);
    };
    edit.state.proposals = receipts.values();
    try boundedState(a, edit.state);
    return projected;
}
pub fn projectReceipts(a: std.mem.Allocator, queue: []const Proposal, receipts: []const Proposal) ![]const Proposal {
    var by_id: std.StringHashMapUnmanaged(Proposal) = .empty;
    for (receipts) |receipt| if (receipt.state != .open) try by_id.put(a, receipt.id, receipt);
    const records = try a.dupe(Proposal, queue);
    for (records) |*record| if (record.state == .open) {
        if (by_id.get(record.id)) |receipt| {
            record.state = receipt.state;
            record.decided = receipt.decided;
        }
    };
    return records;
}

pub fn decisionRequest(proposal: Proposal, input: ProposalDecision) Request {
    const options = if (!input.accept) Options{} else input.options orelse proposal.options;
    return .{ .action = if (!input.accept) .distinct else switch (proposal.action) {
        .merge => .merge,
        .unmerge => .unmerge,
        .suppress => .suppress,
        .unsuppress => .unsuppress,
        .distinct => .distinct,
    }, .key = proposal.key, .to = proposal.into, .replace = options.replace, .pin_distinct = options.pin_distinct, .absorbed = options.absorbed, .actor = input.actor, .reason = input.reason };
}

fn mapKeys(a: std.mem.Allocator, map: std.StringHashMapUnmanaged(void)) ![]const []const u8 {
    var result: std.ArrayListUnmanaged([]const u8) = .empty;
    var it = map.keyIterator();
    while (it.next()) |key| try result.append(a, key.*);
    return result.items;
}
fn appendPair(a: std.mem.Allocator, state: *State, from: []const u8, to: []const u8, origin: Origin) !bool {
    try validKey(from);
    try validKey(to);
    if (std.mem.eql(u8, from, to)) return error.InvalidResolverDistinctPair;
    for (state.distinct) |pair| if ((std.mem.eql(u8, from, pair.from) and std.mem.eql(u8, to, pair.to)) or (std.mem.eql(u8, to, pair.from) and std.mem.eql(u8, from, pair.to))) return false;
    const pairs = try a.alloc(Pair, state.distinct.len + 1);
    @memcpy(pairs[0..state.distinct.len], state.distinct);
    pairs[state.distinct.len] = .{ .from = from, .to = to, .origin = origin };
    state.distinct = pairs;
    return true;
}
fn updateEntries(a: std.mem.Allocator, current: []const Entry, keys: []const []const u8, add: bool, origin: Origin) ![]const Entry {
    var entries: std.StringArrayHashMapUnmanaged(Origin) = .empty;
    for (current) |entry| try entries.put(a, entry.key, entry.origin);
    for (keys) |key| {
        if (add) {
            const slot = try entries.getOrPut(a, key);
            if (!slot.found_existing) slot.value_ptr.* = origin;
        } else _ = entries.orderedRemove(key);
    }
    const result = try a.alloc(Entry, entries.count());
    for (entries.keys(), entries.values(), result) |key, value, *entry| entry.* = .{ .key = key, .origin = value };
    return result;
}
/// Public redirect records carry attribution without changing the execution
/// alias representation or its ownership rules.
pub fn redirectsWithOrigins(a: std.mem.Allocator, aliases: []const Redirect, state: State, curated: []const Redirect) ![]const RedirectOrigin {
    var origins: std.StringHashMapUnmanaged(RedirectOrigin) = .empty;
    for (state.redirect_origins) |entry| try origins.put(a, entry.key, entry);
    var runtime: std.StringHashMapUnmanaged(void) = .empty;
    for (curated) |alias| try runtime.put(a, alias.key, {});
    const result = try a.alloc(RedirectOrigin, aliases.len);
    for (aliases, result) |alias, *entry| {
        const fallback: Origin = if (runtime.contains(alias.key)) .{} else .{ .origin = .config };
        const origin = if (origins.get(alias.key)) |old| if (std.mem.eql(u8, old.target, alias.target)) old.origin else fallback else fallback;
        entry.* = .{ .key = alias.key, .target = alias.target, .origin = origin };
    }
    return result;
}

fn editSet(a: std.mem.Allocator, current: []const []const u8, changes: []const []const u8, add: bool, normalized: bool) ![]const []const u8 {
    var set: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (current) |key| try set.put(a, if (normalized) try normalize(a, key) else key, {});
    for (changes) |raw| {
        const key = if (normalized) try normalize(a, raw) else raw;
        try validKey(key);
        if (add) try set.put(a, key, {}) else _ = set.orderedRemove(key);
    }
    const result = try a.dupe([]const u8, set.keys());
    std.mem.sort([]const u8, result, {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.order(u8, x, y) == .lt;
        }
    }.less);
    return result;
}
pub const Index = struct {
    keys: std.StringHashMapUnmanaged(void) = .empty,
    texts: std.StringHashMapUnmanaged(void) = .empty,
    distinct: std.StringHashMapUnmanaged(void) = .empty,
    removed_aliases: std.StringHashMapUnmanaged(void) = .empty,
    min_text_chars: u32 = 0,
    redirects: ?RedirectIndex = null,

    pub fn init(a: std.mem.Allocator, config_json: []const u8, state_json: []const u8) !Index {
        const config = if (config_json.len == 0) Suppression{} else try std.json.parseFromSliceLeaky(Suppression, a, config_json, .{ .allocate = .alloc_always });
        if (state_json.len > max_state_bytes) return error.ResolverCurationLimitExceeded;
        const Policy = struct { keys: []const []const u8 = &.{}, texts: []const []const u8 = &.{}, min_text_chars: u32 = 0 };
        const ExecutionState = struct { version: u32 = 1, suppress: Policy = .{}, distinct: []const struct { from: []const u8, to: []const u8 } = &.{}, removed_aliases: []const []const u8 = &.{} };
        const state = if (state_json.len == 0) ExecutionState{} else try std.json.parseFromSliceLeaky(ExecutionState, a, state_json, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (state.version != 1) return error.InvalidResolverCurationVersion;
        if (state.suppress.keys.len + state.suppress.texts.len + state.distinct.len + state.removed_aliases.len > 100000) return error.ResolverCurationLimitExceeded;
        if (config.keys.len + config.texts.len > 100000) return error.ResolverCurationLimitExceeded;
        var result: Index = .{};
        for ([_]Policy{ .{ .keys = config.keys, .texts = config.texts, .min_text_chars = config.min_text_chars }, state.suppress }) |policy| {
            result.min_text_chars = @max(result.min_text_chars, policy.min_text_chars);
            for (policy.keys) |key| {
                try validKey(key);
                try result.keys.put(a, key, {});
            }
            for (policy.texts) |text| {
                const normalized = try normalize(a, text);
                if (normalized.len == 0) return error.InvalidResolverSuppression;
                try result.texts.put(a, normalized, {});
            }
        }
        for (state.distinct) |pair| {
            try validKey(pair.from);
            try validKey(pair.to);
            if (std.mem.eql(u8, pair.from, pair.to)) return error.InvalidResolverDistinctPair;
            try result.distinct.put(a, try pairKey(a, pair.from, pair.to), {});
        }
        for (state.removed_aliases) |key| {
            try validKey(key);
            try result.removed_aliases.put(a, key, {});
        }
        return result;
    }

    pub fn bindRedirects(self: *Index, a: std.mem.Allocator, redirects: RedirectIndex) !void {
        self.redirects = redirects;
        const original = try a.alloc([]const u8, self.distinct.count());
        var it = self.distinct.keyIterator();
        var i: usize = 0;
        while (it.next()) |key| : (i += 1) original[i] = key.*;
        for (original) |key| {
            const split = std.mem.indexOfScalar(u8, key, 0).?;
            try self.distinct.put(a, try pairKey(a, redirects.canonical(key[0..split]), redirects.canonical(key[split + 1 ..])), {});
        }
    }
    pub fn textReason(self: Index, a: std.mem.Allocator, text: []const u8) !?Reason {
        const trimmed = std.mem.trim(u8, text, " \t\r\n");
        const chars = std.unicode.utf8CountCodepoints(trimmed) catch return error.InvalidResolverSuppression;
        if (chars < self.min_text_chars) return .min_text_chars;
        if (self.texts.count() > 0 and self.texts.contains(try normalize(a, text))) return .text;
        return null;
    }
    pub fn keySuppressed(self: Index, key: []const u8) bool {
        return self.keys.contains(key);
    }
    pub fn pinnedDistinct(self: Index, a: std.mem.Allocator, from: []const u8, to: []const u8) !bool {
        if (self.distinct.count() == 0) return false;
        const x = if (self.redirects) |r| r.canonical(from) else from;
        const y = if (self.redirects) |r| r.canonical(to) else to;
        return self.distinct.contains(try pairKey(a, x, y));
    }
};

pub fn parseState(a: std.mem.Allocator, json: []const u8) !State {
    if (json.len > max_state_bytes) return error.ResolverCurationLimitExceeded;
    const value = if (json.len == 0) State{} else try std.json.parseFromSliceLeaky(State, a, json, .{ .allocate = .alloc_always });
    if (value.proposals.len > 4096) return error.ResolverCurationLimitExceeded;
    if (value.version != 1) return error.InvalidResolverCurationVersion;
    if (value.suppress.keys.len + value.suppress.texts.len + value.distinct.len + value.removed_aliases.len > 100000) return error.ResolverCurationLimitExceeded;
    const result = value;
    // Attribution is sparse. Reading a key-only policy must not manufacture
    // a full Origin for every key or inflate its next catalog publication.
    for (result.suppress.entries) |entry| {
        try validKey(entry.key);
        try entry.origin.validate();
    }
    for (result.redirect_origins) |entry| {
        try validKey(entry.key);
        try validKey(entry.target);
        try entry.origin.validate();
    }
    for (result.distinct) |pair| if (pair.origin) |origin| try origin.validate();
    if (result.receipt_scope) |scope| for (scope) |byte| if (!std.ascii.isHex(byte)) return error.InvalidResolverCurationRequest;
    var ids: std.StringHashMapUnmanaged(void) = .empty;
    for (result.proposals) |proposal| {
        if ((try ids.getOrPut(a, proposal.id)).found_existing) return error.InvalidResolverCurationRequest;
        if (!std.mem.eql(u8, proposal.id, try proposalId(a, .{ .action = proposal.action, .key = proposal.key, .into = proposal.into }))) return error.InvalidResolverCurationRequest;
        if (proposal.evidence) |evidence| try validateEvidence(a, evidence);
        if (proposal.confidence) |confidence| if (!std.math.isFinite(confidence) or confidence < 0 or confidence > 1) return error.InvalidResolverCurationRequest;
        try validateProposalOrigin(proposal.origin);
        if (proposal.decided) |origin| try validateProposalOrigin(origin);
        if (proposal.state == .open and proposal.decided != null) return error.InvalidResolverCurationRequest;
    }
    return result;
}

pub fn validateConfig(a: std.mem.Allocator, json: []const u8) !void {
    if (json.len > max_state_bytes) return error.ResolverCurationLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    _ = try Index.init(arena.allocator(), json, "");
}
pub fn normalize(a: std.mem.Allocator, text: []const u8) ![]const u8 {
    var output: std.ArrayListUnmanaged(u8) = .empty;
    try slug.append(a, &output, text);
    return output.items;
}
pub fn pairKey(a: std.mem.Allocator, from: []const u8, to: []const u8) ![]const u8 {
    const first = std.mem.order(u8, from, to) == .lt;
    return std.fmt.allocPrint(a, "{s}\x00{s}", .{ if (first) from else to, if (first) to else from });
}
pub fn validKey(key: []const u8) !void {
    if (key.len == 0 or key.len > 16384 or std.mem.indexOfScalar(u8, key, 0) != null or !std.unicode.utf8ValidateSlice(key)) return error.InvalidResolverCurationKey;
}

test "curation indexes normalized text scalar lengths and symmetric distinct pairs" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const index = try Index.init(a,
        \\{"keys":["concept/note"],"texts":["Content"],"min_text_chars":3}
    ,
        \\{"distinct":[{"from":"a","to":"b"}],"suppress":{"texts":["Information"]}}
    );
    try std.testing.expect(index.keySuppressed("concept/note"));
    try std.testing.expectEqual(Reason.text, (try index.textReason(a, " CONTENT ")).?);
    try std.testing.expectEqual(Reason.text, (try index.textReason(a, "information")).?);
    try std.testing.expectEqual(Reason.min_text_chars, (try index.textReason(a, "漢字")).?);
    try std.testing.expect(try index.pinnedDistinct(a, "b", "a"));
    try std.testing.expect(!try index.pinnedDistinct(a, "a", "c"));
}

test "curation plans undo one redirect preserve chains and pin the old pair" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const aliases = [_]Redirect{ .{ .key = "a", .target = "b" }, .{ .key = "b", .target = "c" } };
    const undo = try plan(a, "", &aliases, &aliases, .{ .action = .unmerge, .from = "b", .pin_distinct = true });
    try std.testing.expect(undo.changed);
    try std.testing.expectEqual(@as(usize, 1), undo.merges.len);
    try std.testing.expectEqualStrings("b", undo.merges[0].target);
    try std.testing.expectEqualStrings("b", undo.state.distinct[0].from);
    const absent = try plan(a, "", &.{}, &.{}, .{ .action = .unmerge, .from = "missing" });
    try std.testing.expect(!absent.changed);
    try std.testing.expectError(error.ResolverRedirectExists, plan(a, "", &aliases, &aliases, .{ .action = .merge, .from = "a", .to = "d" }));
    const replace = try plan(a, "", &aliases, &aliases, .{ .action = .merge, .from = "a", .to = "d", .replace = true, .pin_distinct = true });
    try std.testing.expectEqualStrings("d", replace.merges[0].target);
    const suppress = try plan(a, "", &.{}, &.{}, .{ .action = .suppress, .texts = &.{"Note"}, .keys = &.{"concept/note"} });
    const raw = try std.json.Stringify.valueAlloc(a, suppress.state, .{});
    const duplicate = try plan(a, raw, &.{}, &.{}, .{ .action = .suppress, .texts = &.{" note "} });
    try std.testing.expect(!duplicate.changed);
    const remove = try plan(a, raw, &.{}, &.{}, .{ .action = .unsuppress, .texts = &.{"NOTE"} });
    try std.testing.expectEqual(@as(usize, 0), remove.state.suppress.texts.len);
}

test "curation redirect index compresses a long chain and rejects cycles" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const aliases = try a.alloc(Redirect, 10000);
    for (aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "key/{d}", .{i}), .target = try std.fmt.allocPrint(a, "key/{d}", .{i + 1}) };
    const index = try RedirectIndex.init(a, aliases);
    try std.testing.expectEqualStrings("key/10000", index.canonical("key/0"));
    try std.testing.expectEqual(@as(usize, 10000), index.canonical_keys.count());
    aliases[9999].target = "key/0";
    try std.testing.expectError(error.InvalidResolverAliasCycle, RedirectIndex.init(a, aliases));
}

test "curation distinct pins follow both redirect components" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var index = try Index.init(a, "", "{\"version\":1,\"distinct\":[{\"from\":\"a\",\"to\":\"b\"}]}");
    try index.bindRedirects(a, try RedirectIndex.init(a, &.{ .{ .key = "a", .target = "d" }, .{ .key = "b", .target = "c" } }));
    try std.testing.expect(try index.pinnedDistinct(a, "a", "c"));
    try std.testing.expect(try index.pinnedDistinct(a, "d", "b"));
    try std.testing.expect(!(try index.pinnedDistinct(a, "a", "d")));
}

test "curation repoint with a distinct pin is idempotent on retry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original = [_]Redirect{.{ .key = "a", .target = "b" }};
    const intent: Request = .{ .action = .merge, .from = "a", .to = "c", .replace = true, .pin_distinct = true };
    const first = try plan(a, "", &original, &original, intent);
    try std.testing.expect(first.changed);
    const retry = try plan(a, try std.json.Stringify.valueAlloc(a, first.state, .{}), first.merges, first.merges, intent);
    try std.testing.expect(!retry.changed);
    try std.testing.expectEqual(@as(usize, 1), retry.state.distinct.len);
}

test "curation entity inspection scopes policy to the complete redirect component" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const aliases = [_]Redirect{ .{ .key = "a", .target = "b" }, .{ .key = "c", .target = "b" }, .{ .key = "private", .target = "elsewhere" } };
    const state: State = .{
        .suppress = .{ .keys = &.{ "a", "private" }, .texts = &.{"private text"} },
        .distinct = &.{ .{ .from = "c", .to = "d" }, .{ .from = "private", .to = "secret" } },
        .removed_aliases = &.{ "b", "secret" },
    };
    const scoped = try entityState(a, state, &aliases, "a");
    try std.testing.expectEqualStrings("b", scoped.canonical_key);
    try std.testing.expectEqual(@as(usize, 2), scoped.redirects.len);
    try std.testing.expectEqualSlices([]const u8, &.{"a"}, scoped.curation.suppress.keys);
    try std.testing.expectEqual(@as(usize, 0), scoped.curation.suppress.texts.len);
    try std.testing.expectEqual(@as(usize, 1), scoped.curation.distinct.len);
    try std.testing.expectEqualStrings("d", scoped.curation.distinct[0].to);
    try std.testing.expectEqualSlices([]const u8, &.{"b"}, scoped.curation.removed_aliases);
}

test "curation suppress absorbed chains atomically with cascade provenance and explicit release" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const aliases = [_]Redirect{ .{ .key = "a", .target = "b" }, .{ .key = "b", .target = "c" }, .{ .key = "other", .target = "elsewhere" } };
    try std.testing.expectError(error.SuppressedResolverRedirectTarget, plan(a, "", &aliases, &aliases, .{ .action = .suppress, .key = "c" }));
    const stamp: Origin = .{ .actor = "curator", .reason = "generic", .created_at_ms = 123, .revision = 7 };
    const previous = try std.json.Stringify.valueAlloc(a, State{ .redirect_origins = &.{ .{ .key = "a", .target = "b", .origin = stamp }, .{ .key = "b", .target = "c", .origin = stamp }, .{ .key = "other", .target = "elsewhere", .origin = stamp } } }, .{});
    const cascade = try planWithOrigin(a, previous, &aliases, &aliases, .{ .action = .suppress, .key = "c", .absorbed = .suppress }, stamp);
    try std.testing.expectEqual(@as(usize, 2), cascade.absorbed.len);
    try std.testing.expectEqual(@as(usize, 1), cascade.merges.len);
    try std.testing.expectEqual(@as(usize, 1), cascade.state.redirect_origins.len);
    try std.testing.expectEqualStrings("other", cascade.state.redirect_origins[0].key);
    try std.testing.expectEqual(@as(usize, 3), cascade.state.suppress.keys.len);
    try std.testing.expectEqual(@as(usize, 2), cascade.state.removed_aliases.len);
    for (cascade.state.suppress.entries) |entry| {
        try std.testing.expectEqual(@as(u64, 123), entry.origin.created_at_ms);
        try std.testing.expectEqual(@as(u64, 7), entry.origin.revision);
        try std.testing.expectEqualStrings("curator", entry.origin.actor);
        if (!std.mem.eql(u8, entry.key, "c")) {
            try std.testing.expectEqual(.cascade, entry.origin.origin);
            try std.testing.expectEqualStrings("c", entry.origin.cascade_from.?);
        }
    }
    const undo = try plan(a, try std.json.Stringify.valueAlloc(a, cascade.state, .{}), cascade.merges, cascade.merges, .{ .action = .unsuppress, .key = "c" });
    try std.testing.expectEqual(@as(usize, 2), undo.state.suppress.keys.len);
    try std.testing.expectEqual(@as(usize, 1), undo.merges.len);
    const release = try planWithOrigin(a, "", &aliases, &aliases, .{ .action = .suppress, .key = "c", .absorbed = .release, .pin_distinct = true }, stamp);
    try std.testing.expectEqual(@as(usize, 2), release.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 1), release.state.suppress.keys.len);
    const retry = try planWithOrigin(a, try std.json.Stringify.valueAlloc(a, release.state, .{}), release.merges, release.merges, .{ .action = .suppress, .key = "c", .absorbed = .release, .pin_distinct = true }, .{ .created_at_ms = 999 });
    try std.testing.expect(!retry.changed);
    try std.testing.expectEqual(@as(u64, 123), retry.state.suppress.entries[0].origin.created_at_ms);
}

test "curation proposals retain rejection identity and atomically accept review actions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const input: ProposalRequest = .{ .action = .merge, .key = "a", .into = "b", .confidence = 0.8, .actor = "screen" };
    const created = try propose(a, .{}, &.{}, input, .{ .actor = input.actor, .created_at_ms = 5 });
    const id = created.state.proposals[0].id;
    const rejected = try decide(a, created.state, &.{}, &.{}, id, .{ .accept = false, .pin_distinct = true }, .{ .actor = "user", .revision = 1 });
    try std.testing.expectEqual(.rejected, rejected.state.proposals[0].state);
    try std.testing.expectEqual(.review, rejected.state.distinct[0].origin.?.origin);
    try std.testing.expectEqualStrings(id, rejected.state.distinct[0].origin.?.proposal_id.?);
    const repeated = try propose(a, rejected.state, &.{}, input, .{});
    try std.testing.expect(!repeated.changed);
    try std.testing.expectEqual(.rejected, repeated.state.proposals[0].state);
    try std.testing.expectError(error.ResolverCurationConflict, decide(a, rejected.state, &.{}, &.{}, id, .{ .accept = true }, .{}));
    const accepted = try decide(a, created.state, &.{}, &.{}, id, .{ .accept = true }, .{ .created_at_ms = 10, .revision = 2 });
    try std.testing.expectEqual(.accepted, accepted.state.proposals[0].state);
    try std.testing.expectEqualStrings("b", accepted.merges[0].target);
    try std.testing.expectEqual(.review, accepted.state.redirect_origins[0].origin.origin);
    const retry = try decide(a, accepted.state, accepted.merges, accepted.merges, id, .{ .accept = true }, .{ .created_at_ms = 99 });
    try std.testing.expect(!retry.changed);
    try std.testing.expectEqual(@as(u64, 10), retry.state.proposals[0].decided.?.created_at_ms);
    const direct = try planWithOrigin(a, try std.json.Stringify.valueAlloc(a, created.state, .{}), &.{}, &.{}, .{ .action = .merge, .key = "a", .to = "elsewhere" }, .{});
    try std.testing.expectEqual(.superseded, direct.state.proposals[0].state);
}

test "curation proposal keyset pages preserve bounded filters and symmetric distinct identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first = try propose(a, .{}, &.{}, .{ .action = .suppress, .key = "a" }, .{});
    const second = try propose(a, first.state, &.{}, .{ .action = .suppress, .key = "b" }, .{});
    const third = try propose(a, second.state, &.{}, .{ .action = .merge, .key = "c", .into = "d" }, .{});
    const page = try proposalPage(a, third.state, "state=open&action=suppress&limit=1");
    try std.testing.expectEqual(@as(usize, 1), page.proposals.len);
    const next = try proposalPage(a, third.state, try std.fmt.allocPrint(a, "state=open&action=suppress&limit=1&cursor={s}", .{page.next_cursor.?}));
    try std.testing.expectEqual(@as(usize, 1), next.proposals.len);
    try std.testing.expect(next.next_cursor == null);
    try std.testing.expect(!std.mem.eql(u8, page.proposals[0].id, next.proposals[0].id));
    try std.testing.expectEqualStrings(try proposalId(a, .{ .action = .distinct, .key = "a", .into = "b" }), try proposalId(a, .{ .action = .distinct, .key = "b", .into = "a" }));
    try std.testing.expectError(error.InvalidResolverCurationRequest, proposalPage(a, third.state, "limit=0"));
    try std.testing.expectError(error.InvalidResolverCurationRequest, propose(a, .{}, &.{}, .{ .action = .suppress, .key = "a", .confidence = std.math.nan(f64) }, .{}));
    try std.testing.expectError(error.InvalidResolverCurationRequest, propose(a, .{}, &.{}, .{ .action = .merge, .key = "a" }, .{}));
}

test "curation absorbed disposition handles intermediate targets and large distinct batches" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const chain = [_]Redirect{ .{ .key = "a", .target = "b" }, .{ .key = "b", .target = "c" } };
    const edit = try plan(a, "", &chain, &chain, .{ .action = .suppress, .key = "b", .absorbed = .suppress });
    try std.testing.expectEqual(@as(usize, 1), edit.merges.len);
    try std.testing.expectEqualStrings("b", edit.merges[0].key);
    try std.testing.expectEqualStrings("c", edit.merges[0].target);
    try std.testing.expectEqual(@as(usize, 2), edit.state.suppress.keys.len);
    const aliases = try a.alloc(Redirect, 5000);
    for (aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "source/{d}", .{i}), .target = "target" };
    const released = try plan(a, "", aliases, aliases, .{ .action = .suppress, .key = "target", .absorbed = .release, .pin_distinct = true });
    try std.testing.expectEqual(@as(usize, 5000), released.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 0), released.merges.len);
}

test "curation supersession follows both entity references and symmetric merge judgments" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var queue: State = .{};
    for ([_]ProposalRequest{
        .{ .action = .merge, .key = "a", .into = "b" },
        .{ .action = .suppress, .key = "a" },
        .{ .action = .distinct, .key = "a", .into = "b" },
        .{ .action = .merge, .key = "x", .into = "a" },
        .{ .action = .unmerge, .key = "x" },
    }) |request| queue = (try propose(a, queue, &.{}, request, .{})).state;
    const merged = try supersede(a, queue.proposals, .{ .action = .merge, .key = "a", .to = "elsewhere" }, &.{}, .{});
    for (merged[0..4]) |proposal| try std.testing.expectEqual(.superseded, proposal.state);
    try std.testing.expectEqual(.open, merged[4].state);
    const pinned = try supersede(a, queue.proposals, .{ .action = .distinct, .key = "b", .to = "a" }, &.{}, .{});
    try std.testing.expectEqual(.superseded, pinned[0].state);
    try std.testing.expectEqual(.superseded, pinned[2].state);
    try std.testing.expectEqual(.open, pinned[1].state);
    const suppressed = try supersede(a, queue.proposals, .{ .action = .suppress, .key = "b" }, &.{}, .{});
    try std.testing.expectEqual(.superseded, suppressed[0].state);
    try std.testing.expectEqual(.superseded, suppressed[2].state);
    try std.testing.expectEqual(.open, suppressed[3].state);
}

test "curation receipt scopes distinguish queue lifetimes and preserve retries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const first: ReceiptScope = @splat('a');
    const second: ReceiptScope = @splat('b');
    const request: ProposalRequest = .{ .action = .suppress, .key = "concept/x" };
    const queued = try propose(a, .{ .receipt_scope = first }, &.{}, request, .{});
    const id = queued.state.proposals[0].id;
    const accepted = try decide(a, queued.state, &.{}, &.{}, id, .{ .accept = true }, .{});
    const corrected = try plan(a, try std.json.Stringify.valueAlloc(a, accepted.state, .{}), &.{}, &.{}, .{ .action = .unsuppress, .key = "concept/x" });
    const retried = try decide(a, bindReceiptScope(corrected.state, first), &.{}, &.{}, id, .{ .accept = true }, .{});
    try std.testing.expect(!retried.changed and retried.state.suppress.keys.len == 0);
    var rebound = bindReceiptScope(corrected.state, second);
    try std.testing.expectEqual(@as(usize, 0), rebound.proposals.len);
    const fresh = try propose(a, .{ .receipt_scope = second }, &.{}, request, .{});
    try std.testing.expectEqualStrings(id, fresh.state.proposals[0].id);
    rebound.proposals = fresh.state.proposals;
    const applied = try decide(a, rebound, &.{}, &.{}, id, .{ .accept = true }, .{});
    try std.testing.expectEqual(@as(usize, 1), applied.state.suppress.keys.len);
    const restored = try parseState(a, try std.json.Stringify.valueAlloc(a, applied.state, .{}));
    try std.testing.expect(sameReceiptScope(second, restored.receipt_scope));
}

test "curation sparse attribution keeps large key and text policies editable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const keys = try a.alloc([]const u8, 40000);
    for (keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(a, "concept/key-{d}", .{i});
    const texts = try a.alloc([]const u8, 10000);
    for (texts, 0..) |*text, i| text.* = try std.fmt.allocPrint(a, "text {d}", .{i});
    const bytes = try std.json.Stringify.valueAlloc(a, State{ .suppress = .{ .keys = keys, .texts = texts } }, .{});
    const parsed = try parseState(a, bytes);
    try std.testing.expectEqual(@as(usize, 0), parsed.suppress.entries.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.suppress.text_entries.len);
    try std.testing.expectEqual(bytes.len, jsonSize(parsed));
    const edit = try plan(a, bytes, &.{}, &.{}, .{ .action = .unsuppress, .key = keys[0] });
    try std.testing.expect(edit.changed);
    try std.testing.expectEqual(keys.len - 1, edit.state.suppress.keys.len);
    try std.testing.expectEqual(@as(usize, 0), edit.state.suppress.entries.len);
    const asserted = try planWithOrigin(a, bytes, &.{}, &.{}, .{ .action = .suppress, .key = "concept/new" }, .{ .actor = "curator" });
    try std.testing.expectEqual(@as(usize, 1), asserted.state.suppress.entries.len);
    try std.testing.expectEqualStrings("curator", asserted.state.suppress.entries[0].origin.actor);
}

test "curation queue reserves escaped decision attribution for every open proposal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var evidence: std.json.ObjectMap = .empty;
    const payload = try a.alloc(u8, 14000);
    @memset(payload, 'x');
    try evidence.put(a, "data", .{ .string = payload });
    var state: State = .{ .receipt_scope = @splat('a') };
    var count: usize = 0;
    while (true) : (count += 1) {
        const request: ProposalRequest = .{ .action = .suppress, .key = try std.fmt.allocPrint(a, "concept/{d}", .{count}), .evidence = .{ .object = evidence } };
        const next = propose(a, state, &.{}, request, .{}) catch |err| {
            try std.testing.expectEqual(error.ResolverCurationLimitExceeded, err);
            break;
        };
        state = next.state;
    }
    try std.testing.expect(count > 1 and count < 4096);
    try validateProposalQueue(state);
    const reservation = proposalCapacityBytes(state);
    const actor: [256]u8 = @splat(0);
    const reason: [4096]u8 = @splat(0);
    const stamp: Origin = .{ .actor = &actor, .reason = &reason, .created_at_ms = std.math.maxInt(u64), .revision = std.math.maxInt(u64) };
    const superseded = try supersede(a, state.proposals, .{ .action = .suppress, .keys = blk: {
        const keys = try a.alloc([]const u8, state.proposals.len);
        for (state.proposals, keys) |proposal, *key| key.* = proposal.key;
        break :blk keys;
    } }, &.{}, stamp);
    var all_settled = state;
    all_settled.proposals = superseded;
    try validateProposalQueue(all_settled);
    for (superseded) |proposal| try std.testing.expectEqual(.superseded, proposal.state);
    // Rejections are always possible, including when every decision has the
    // largest legal attribution and every byte requires JSON escaping.
    for (0..count) |i| {
        if (i == count - 1) {
            const pending = try std.json.Stringify.valueAlloc(a, ProposalManifest{
                .queue = state,
                .pending_id = state.proposals[i].id,
                .pending_decision = .{ .accept = true, .actor = &actor, .reason = &reason },
                .pending_origin = stamp,
            }, .{});
            try std.testing.expect(pending.len <= max_manifest_bytes);
            _ = try parseProposalManifest(a, pending);
        }
        state = (try decide(a, state, &.{}, &.{}, state.proposals[i].id, .{ .accept = false }, stamp)).state;
        try std.testing.expect(proposalCapacityBytes(state) <= reservation);
    }
    for (state.proposals) |proposal| {
        try std.testing.expectEqual(.rejected, proposal.state);
        try std.testing.expect(jsonSize(proposal.decided.?) <= decision_origin_byte_budget);
    }
    const manifest = try std.json.Stringify.valueAlloc(a, ProposalManifest{ .queue = state }, .{});
    const reopened = try parseProposalManifest(a, manifest);
    try std.testing.expectEqual(count, reopened.queue.proposals.len);
    try std.testing.expectEqual(jsonSize(state), jsonSize(reopened.queue));
}

test "curation refresh replaces only open proposals and preserves queue identity and terminal decisions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var request: ProposalRequest = .{ .action = .merge, .key = "a", .into = "b" };
    const initial = try propose(a, .{ .receipt_scope = @splat(7) }, &.{}, request, .{ .actor = "first", .revision = 1 });
    request.options.replace = true;
    const next = try refreshProposal(a, initial.state, &.{}, request, .{ .actor = "second", .revision = 2 });
    try std.testing.expectEqualStrings(initial.state.proposals[0].id, next.state.proposals[0].id);
    try std.testing.expect(next.state.proposals[0].options.replace);
    try std.testing.expectEqualStrings("second", next.state.proposals[0].origin.actor);
    try std.testing.expect(sameReceiptScope(initial.state.receipt_scope, next.state.receipt_scope));
    try std.testing.expectError(error.ResolverCurationConflict, refreshProposal(a, next.state, &.{}, request, .{ .revision = 2 }));
    for ([_]ProposalState{ .accepted, .rejected, .superseded }) |terminal| {
        var entry = next.state.proposals[0];
        entry.state = terminal;
        var state = next.state;
        state.proposals = &.{entry};
        try std.testing.expectError(error.ResolverCurationConflict, refreshProposal(a, state, &.{}, request, .{ .revision = 3 }));
    }
    request.options.absorbed = .release;
    try std.testing.expectError(error.InvalidResolverCurationRequest, refreshProposal(a, next.state, &.{}, request, .{ .revision = 3 }));
}
