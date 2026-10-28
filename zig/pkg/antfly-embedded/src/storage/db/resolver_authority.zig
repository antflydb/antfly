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

//! Native destination-owned curation records. The enclosing native transaction
//! owns revision comparison, record/index edits and publication of its head.
//! A single-entity read touches its redirect component and incident policy only.
pub const routes = @import("resolver_namespace_routes.zig");
const std = @import("std");
const builtin = @import("builtin");
const resolver = @import("antfly_resolver");
const c = resolver.curation;
const policy = @import("resolver_namespace.zig");
const A = std.mem.Allocator;
var test_pin_summary_probes: std.atomic.Value(usize) = .init(0);
var test_tail_forward_steps: std.atomic.Value(usize) = .init(0);
pub const static_profiles = @import("resolver_static_profiles.zig");
pub const query_context = @import("resolver_query_context.zig");
const dependency_filter = @import("resolver_dependency_filter.zig");
pub const lite_prefix = "\x00\x00__metadata__:resolver_authority:v1:";
pub const lite_marker = lite_prefix ++ "version";
pub const Row = struct { key: []const u8, value: []const u8 };
pub const Page = struct { rows: []const Row, next: ?[]const u8 = null };
pub const Backend = struct {
    ptr: *anyopaque,
    get: *const fn (*anyopaque, A, []const u8) anyerror!?[]const u8,
    put: *const fn (*anyopaque, A, []const u8, []const u8) anyerror!void,
    remove: *const fn (*anyopaque, A, []const u8) anyerror!void,
    scan: *const fn (*anyopaque, A, []const u8, ?[]const u8, u32) anyerror!Page,
};
pub const Stamp = struct { revision: u64, fingerprint: [32]u8 };
pub const Head = struct {
    version: u8 = 1,
    key_template: []const u8,
    revision: u64,
    adoption_revision: ?u64 = null,
    adoption_standalone: bool = false,
    min_text_chars: u32 = 0,
    bytes: u64 = 0,
    /// State lineage, including adoption, fences immutable projection caches
    /// across snapshot replacement and divergent equal-revision histories.
    fingerprint: [32]u8 = @splat(0),
};
pub const Kind = enum { redirect, suppressed, suppression, distinct, redirect_origin, removed_alias, text, text_origin };
pub const Edit = struct { kind: Kind, id: []const u8, value: ?std.json.Value };
pub const Patch = struct { expected_revision: u64, next: Head, edits: []const Edit };
pub const QueueHead = struct { version: u8 = 1, revision: u64 = 0, count: u32 = 0, bytes: u64 = 0, receipt_scope: ?c.ReceiptScope = null };
pub const QueuePatch = struct { expected_revision: u64, revision: u64, receipt_scope: ?c.ReceiptScope = null, proposals: []const c.Proposal = &.{}, retain_evidence: bool = false };
const ProposalHeader = struct {
    version: u8 = 1,
    proposal: c.Proposal,
    evidence_hash: [32]u8,
    capacity: u64,
};
fn evidenceHash(a: A, value: ?std.json.Value) ![32]u8 {
    var result: [32]u8 = undefined;
    @import("antfly_hash").Sha256.hash(try encode(a, value), &result, .{});
    return result;
}
fn proposalHeaderValue(a: A, value: c.Proposal) !ProposalHeader {
    var header = value;
    header.evidence = null;
    return .{ .proposal = header, .evidence_hash = try evidenceHash(a, value.evidence), .capacity = c.proposalCapacityBytes(.{ .proposals = &.{value} }) };
}
pub const EdgeMask = struct { ptr: *anyopaque, enabled: *const fn (*anyopaque, A, []const u8) anyerror!bool };
pub const AliasLookup = struct {
    ptr: *anyopaque,
    expand: *const fn (*anyopaque, A, []const u8, bool, EdgeMask) anyerror![]const resolver.KeyAlias,
    node: ?*const fn (*anyopaque, A, []const u8) anyerror!?query_context.QueryNode = null,
    seek_node: ?*const fn (*anyopaque, A, []const u8, ?[]const u8) anyerror!?query_context.QueryNode = null,
    profile_active: ?*const fn (*anyopaque, A, []const u8) anyerror!bool = null,
    profile_epoch: ?*const fn (*anyopaque, A) anyerror!u64 = null,
};
pub const AliasProfile = struct { id: []const u8 = "", key_template: []const u8, aliases: []const resolver.KeyAlias, suppress_json: []const u8 = "" };
/// A request-local key: modern bindings already carry immutable static
/// identity. Only legacy curated overlays need additional content hashing.
pub fn admissionProfileKey(a: A, cfg: anytype) ![]const u8 {
    if (cfg.admission_profile.len > 0 and cfg.merges.len == 0 and cfg.curation_json.len == 0) return cfg.admission_profile;
    var buffer: [1024]u8 = undefined;
    var writer: std.Io.Writer.Hashing(@import("antfly_hash").Sha256) = .init(&buffer);
    try std.json.Stringify.value(.{ cfg.admission_profile, if (cfg.admission_profile.len == 0) cfg.aliases else &.{}, if (cfg.admission_profile.len == 0) cfg.suppress_json else "", cfg.key_template, cfg.merges, cfg.curation_json }, .{}, &writer.writer);
    try writer.writer.flush();
    var digest: [32]u8 = undefined;
    writer.hasher.final(&digest);
    return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
pub fn effectiveAliases(a: A, namespace: policy.Namespace, profiles: []const AliasProfile, key: []const u8, into: ?[]const u8) ![]const resolver.KeyAlias {
    const Edge = struct { target: []const u8, conflict: bool = false };
    var authoritative: std.StringHashMapUnmanaged(void) = .empty;
    var removed: std.StringHashMapUnmanaged(void) = .empty;
    var edges: std.StringHashMapUnmanaged(Edge) = .empty;
    var adjacent: std.StringHashMapUnmanaged(std.ArrayList([]const u8)) = .empty;
    for (namespace.merges) |alias| try authoritative.put(a, alias.key, {});
    for (namespace.state.removed_aliases) |id| try removed.put(a, id, {});
    var aliases: std.ArrayList(resolver.KeyAlias) = .empty;
    for (profiles) |profile| if (std.mem.eql(u8, profile.key_template, namespace.key_template)) {
        for (profile.aliases) |alias| if (!authoritative.contains(alias.key) and !removed.contains(alias.key)) try aliases.append(a, alias);
    };
    try aliases.appendSlice(a, namespace.merges);
    for (aliases.items) |alias| {
        const slot = try edges.getOrPut(a, alias.key);
        if (!slot.found_existing) slot.value_ptr.* = .{ .target = alias.target } else if (!std.mem.eql(u8, slot.value_ptr.target, alias.target)) slot.value_ptr.conflict = true;
        const outgoing = try adjacent.getOrPut(a, alias.key);
        if (!outgoing.found_existing) outgoing.value_ptr.* = .empty;
        try outgoing.value_ptr.append(a, alias.target);
        const incoming = try adjacent.getOrPut(a, alias.target);
        if (!incoming.found_existing) incoming.value_ptr.* = .empty;
        try incoming.value_ptr.append(a, alias.key);
    }
    var pending: std.ArrayList([]const u8) = .empty;
    try pending.append(a, key);
    if (into) |target| try pending.append(a, target);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var offset: usize = 0;
    while (offset < pending.items.len) : (offset += 1) {
        const id = pending.items[offset];
        if ((try seen.getOrPut(a, id)).found_existing) continue;
        if (seen.count() > 16384) return error.ResolverCurationLimitExceeded;
        if (adjacent.get(id)) |neighbors| try pending.appendSlice(a, neighbors.items);
    }
    var result: std.ArrayList(resolver.KeyAlias) = .empty;
    var iterator = edges.iterator();
    while (iterator.next()) |edge| if (seen.contains(edge.key_ptr.*)) {
        if (edge.value_ptr.conflict) return error.ResolverNamespaceMigrationConflict;
        try result.append(a, .{ .key = edge.key_ptr.*, .target = edge.value_ptr.target });
    };
    return result.items;
}

/// A repeated source-local declaration is still a new shared assertion. Let
/// the planner record its origin rather than taking the static-alias retry path.
pub fn planningAliases(a: A, namespace: policy.Namespace, effective: []const resolver.KeyAlias, action: c.Action, key: []const u8, into: ?[]const u8) ![]const resolver.KeyAlias {
    if (action != .merge or into == null) return effective;
    for (namespace.merges) |alias| if (std.mem.eql(u8, alias.key, key)) return effective;
    for (effective) |alias| if (std.mem.eql(u8, alias.key, key) and std.mem.eql(u8, alias.target, into.?)) {
        var filtered: std.ArrayList(resolver.KeyAlias) = .empty;
        for (effective) |item| if (!std.mem.eql(u8, item.key, key)) try filtered.append(a, item);
        return filtered.items;
    };
    return effective;
}

/// An overlay only adds outgoing edges at terminal nodes of the validated
/// curated graph. Base paths can therefore jump straight to their terminal;
/// profile-local compression never copies the curated canonical map.
const ProfileRedirects = struct {
    base: c.RedirectIndex,
    canonical_keys: std.StringHashMapUnmanaged([]const u8) = .empty,
    fn init(a: A, base: c.RedirectIndex, aliases: []const resolver.KeyAlias) !ProfileRedirects {
        var result: ProfileRedirects = .{ .base = base };
        var edges: std.StringHashMapUnmanaged([]const u8) = .empty;
        for (aliases) |alias| try edges.put(a, alias.key, alias.target);
        var path: std.ArrayList([]const u8) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (aliases) |alias| {
            path.clearRetainingCapacity();
            seen.clearRetainingCapacity();
            var current = alias.key;
            const target = walk: while (true) {
                if (result.canonical_keys.get(current)) |known| break :walk known;
                const next = edges.get(current) orelse break :walk current;
                if ((try seen.getOrPut(a, current)).found_existing) return error.InvalidResolverAliasCycle;
                try path.append(a, current);
                current = base.canonical(next);
            };
            for (path.items) |key| try result.canonical_keys.put(a, key, target);
        }
        return result;
    }
    fn canonical(self: ProfileRedirects, key: []const u8) []const u8 {
        const terminal = self.base.canonical(key);
        return self.canonical_keys.get(terminal) orelse terminal;
    }
};
const ProfileValidation = struct {
    namespace: policy.Namespace,
    base: c.RedirectIndex,
    negative: c.Index,
    incoming: std.StringHashMapUnmanaged(void) = .empty,
    blocked: std.ArrayList([]const u8) = .empty,
    distinct: std.StringHashMapUnmanaged(std.ArrayList(c.Pair)) = .empty,
    fn init(a: A, namespace: policy.Namespace) !ProfileValidation {
        var result: ProfileValidation = .{
            .namespace = namespace,
            .base = try c.RedirectIndex.init(a, namespace.merges),
            .negative = try c.Index.init(a, "", try encode(a, namespace.state)),
        };
        for (namespace.merges) |alias| {
            const target = result.base.canonical(alias.target);
            const slot = try result.incoming.getOrPut(a, target);
            if (!slot.found_existing and result.negative.keySuppressed(target)) try result.blocked.append(a, target);
        }
        for (namespace.state.distinct) |pair| {
            const from = result.base.canonical(pair.from);
            const to = result.base.canonical(pair.to);
            if (std.mem.eql(u8, from, to)) return error.ResolverDistinctPair;
            for ([_][]const u8{ from, to }) |endpoint| {
                const slot = try result.distinct.getOrPut(a, endpoint);
                if (!slot.found_existing) slot.value_ptr.* = .empty;
                try slot.value_ptr.append(a, pair);
            }
        }
        return result;
    }
    fn validate(self: ProfileValidation, a: A, static: []const resolver.KeyAlias, suppress_json: []const u8) !void {
        _ = try c.RedirectIndex.init(a, static);
        var added: std.ArrayList(resolver.KeyAlias) = .empty;
        for (static) |alias| if (!self.base.canonical_keys.contains(alias.key) and !self.negative.removed_aliases.contains(alias.key)) try added.append(a, alias);
        const overlay = try ProfileRedirects.init(a, self.base, added.items);
        const local_negative = try c.Index.init(a, suppress_json, "");
        for (self.blocked.items) |terminal| if (std.mem.eql(u8, overlay.canonical(terminal), terminal)) return error.SuppressedResolverRedirectTarget;
        // Every changed curated path ends at the target of an added edge.
        // Unchanged paths were checked once; source-local key rules can check
        // their terminal directly through the shared incoming directory.
        for (added.items) |alias| {
            const target = overlay.canonical(alias.target);
            if (self.negative.keySuppressed(target) or local_negative.keySuppressed(target)) return error.SuppressedResolverRedirectTarget;
            if (self.distinct.get(alias.key)) |pairs| for (pairs.items) |pair| {
                if (std.mem.eql(u8, overlay.canonical(pair.from), overlay.canonical(pair.to))) return error.ResolverDistinctPair;
            };
        }
        var keys = local_negative.keys.keyIterator();
        while (keys.next()) |key| if (std.mem.eql(u8, overlay.canonical(key.*), key.*) and self.incoming.contains(key.*)) return error.SuppressedResolverRedirectTarget;
    }
};
pub fn validateProfile(a: A, namespace: policy.Namespace, static: []const resolver.KeyAlias, suppress_json: []const u8) !void {
    const context = try ProfileValidation.init(a, namespace);
    try context.validate(a, static, suppress_json);
}
pub fn validateProfiles(a: A, namespace: policy.Namespace, profiles: []const AliasProfile) !void {
    const context = try ProfileValidation.init(a, namespace);
    var checked: std.StringHashMapUnmanaged(void) = .empty;
    for (profiles) |profile| if (std.mem.eql(u8, namespace.key_template, profile.key_template)) {
        const key = if (profile.id.len > 0) profile.id else try encode(a, .{ profile.aliases, profile.suppress_json });
        if ((try checked.getOrPut(a, key)).found_existing) continue;
        try context.validate(a, profile.aliases, profile.suppress_json);
    };
}

pub const Snapshot = struct {
    head: Head,
    namespace: policy.Namespace,
    /// Query-only projection: merges include the effective configured paths.
    /// Never persisted as namespace corrections or sent to older readers.
    complete_aliases: bool = false,
    /// Query-only configured negatives, qualified by the enrolled profile.
    configured_suppression: ?c.Suppression = null,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("head");
        try writer.write(self.head);
        try writer.objectField("namespace");
        try writer.write(self.namespace);
        if (self.complete_aliases) {
            try writer.objectField("complete_aliases");
            try writer.write(true);
        }
        if (self.configured_suppression) |suppression| {
            try writer.objectField("configured_suppression");
            try writer.write(suppression);
        }
        try writer.endObject();
    }
};
fn parse(comptime T: type, a: A, bytes: []const u8) !T {
    return std.json.parseFromSliceLeaky(T, a, bytes, .{ .allocate = .alloc_always });
}
fn encode(a: A, value: anytype) ![]const u8 {
    return std.json.Stringify.valueAlloc(a, value, .{});
}
fn hash(a: A, value: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    @import("antfly_hash").Sha256.hash(value, &digest, .{});
    return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
pub var test_query_base_loads: std.atomic.Value(usize) = .init(0);
pub var test_proposal_key_scans: std.atomic.Value(usize) = .init(0);
pub var test_head_enumerations: std.atomic.Value(usize) = .init(0);
pub const Store = struct {
    a: A,
    backend: Backend,
    prefix: []const u8,
    pin_changes: ?*std.StringHashMapUnmanaged(void) = null,
    max_namespace_bytes: ?u64 = c.max_state_bytes,
    max_queue_bytes: ?u64 = c.max_state_bytes,
    max_queue_count: ?u32 = 4096,
    fn keyPath(self: Store, suffix: []const u8) ![]const u8 {
        return std.mem.concat(self.a, u8, &.{ self.prefix, suffix });
    }
    fn headPath(self: Store, template: []const u8) ![]const u8 {
        return self.keyPath(try std.fmt.allocPrint(self.a, "head/{s}", .{try hash(self.a, template)}));
    }
    fn records(self: Store, template: []const u8, kind: ?Kind) ![]const u8 {
        return self.keyPath(if (kind) |k| try std.fmt.allocPrint(self.a, "record/{s}/{s}/", .{ try hash(self.a, template), @tagName(k) }) else try std.fmt.allocPrint(self.a, "record/{s}/", .{try hash(self.a, template)}));
    }
    fn record(self: Store, template: []const u8, kind: Kind, id: []const u8) ![]const u8 {
        return std.mem.concat(self.a, u8, &.{ try self.records(template, kind), try hash(self.a, id) });
    }
    fn adjacent(self: Store, template: []const u8, name: []const u8, id: []const u8) ![]const u8 {
        return self.keyPath(try std.fmt.allocPrint(self.a, "{s}/{s}/{s}/", .{ name, try hash(self.a, template), try hash(self.a, id) }));
    }
    pub fn head(self: Store, template: []const u8) !?Head {
        return if (try self.backend.get(self.backend.ptr, self.a, try self.headPath(template))) |raw| try parse(Head, self.a, raw) else null;
    }
    fn headRoutePrefix(template: []const u8) []const u8 {
        return template[0 .. std.mem.indexOf(u8, template, "{{") orelse template.len];
    }
    fn indexHead(self: Store, h: Head) !void {
        const prefix = headRoutePrefix(h.key_template);
        for (0..prefix.len + 1) |length| {
            const node = try self.keyPath(try std.fmt.allocPrint(self.a, "head-route-node/{s}", .{try hash(self.a, prefix[0..length])}));
            if (length == prefix.len) {
                try self.backend.put(self.backend.ptr, self.a, node, "1");
            } else if (try self.backend.get(self.backend.ptr, self.a, node) == null) try self.backend.put(self.backend.ptr, self.a, node, "0");
        }
        try self.backend.put(self.backend.ptr, self.a, try self.keyPath(try std.fmt.allocPrint(self.a, "head-route/{s}/{s}", .{ try hash(self.a, prefix), try hash(self.a, h.key_template) })), try encode(self.a, h.key_template));
    }
    pub fn ensureHeadRoutes(self: Store) !void {
        const ready = try self.keyPath("head-routes-ready");
        if (try self.backend.get(self.backend.ptr, self.a, ready) != null) return;
        for (try self.heads()) |h| try self.indexHead(h);
        try self.backend.put(self.backend.ptr, self.a, ready, "1");
    }
    pub fn matchingHeads(self: Store, key: []const u8) ![]const Head {
        var result: std.ArrayList(Head) = .empty;
        if (try self.backend.get(self.backend.ptr, self.a, try self.keyPath("head-routes-ready")) == null) {
            // Read-only legacy files remain readable until authorized adoption.
            for (try self.heads()) |h| if (policy.keyInNamespace(h.key_template, key)) try result.append(self.a, h);
            return result.items;
        }
        for (0..key.len + 1) |length| {
            const prefix_hash = try hash(self.a, key[0..length]);
            const node = (try self.backend.get(self.backend.ptr, self.a, try self.keyPath(try std.fmt.allocPrint(self.a, "head-route-node/{s}", .{prefix_hash})))) orelse break;
            if (std.mem.eql(u8, node, "0")) continue;
            if (!std.mem.eql(u8, node, "1")) return error.InvalidCatalogRecord;
            const prefix = try self.keyPath(try std.fmt.allocPrint(self.a, "head-route/{s}/", .{prefix_hash}));
            var after: ?[]const u8 = null;
            while (true) {
                const page = try self.backend.scan(self.backend.ptr, self.a, prefix, after, 128);
                for (page.rows) |row| {
                    const template = try parse([]const u8, self.a, row.value);
                    if (policy.keyInNamespace(template, key)) try result.append(self.a, (try self.head(template)) orelse return error.InvalidCatalogRecord);
                }
                after = page.next;
                if (after == null) break;
            }
        }
        return result.items;
    }
    pub fn heads(self: Store) ![]const Head {
        if (comptime @import("builtin").is_test) _ = test_head_enumerations.fetchAdd(1, .monotonic);
        const prefix = try self.keyPath("head/");
        var result: std.ArrayList(Head) = .empty;
        var after: ?[]const u8 = null;
        while (true) {
            const page = try self.backend.scan(self.backend.ptr, self.a, prefix, after, 128);
            for (page.rows) |row| try result.append(self.a, try parse(Head, self.a, row.value));
            after = page.next;
            if (after == null) break;
            if (result.items.len >= 4096) return error.ResolverCurationLimitExceeded;
        }
        return result.items;
    }
    fn get(self: Store, comptime T: type, template: []const u8, kind: Kind, id: []const u8) !?T {
        return if (try self.backend.get(self.backend.ptr, self.a, try self.record(template, kind, id))) |raw| try parse(T, self.a, raw) else null;
    }
    const Values = struct {
        aliases: std.ArrayList(resolver.KeyAlias) = .empty,
        suppressed: std.ArrayList([]const u8) = .empty,
        entries: std.ArrayList(c.Entry) = .empty,
        distinct: std.ArrayList(c.Pair) = .empty,
        origins: std.ArrayList(c.RedirectOrigin) = .empty,
        removed: std.ArrayList([]const u8) = .empty,
        texts: std.ArrayList([]const u8) = .empty,
        text_entries: std.ArrayList(c.Entry) = .empty,
        fn append(self: *Values, a: A, kind: Kind, raw: []const u8) !void {
            switch (kind) {
                .redirect => try self.aliases.append(a, try parse(resolver.KeyAlias, a, raw)),
                .suppressed => try self.suppressed.append(a, try parse([]const u8, a, raw)),
                .suppression => try self.entries.append(a, try parse(c.Entry, a, raw)),
                .distinct => try self.distinct.append(a, try parse(c.Pair, a, raw)),
                .redirect_origin => try self.origins.append(a, try parse(c.RedirectOrigin, a, raw)),
                .removed_alias => try self.removed.append(a, try parse([]const u8, a, raw)),
                .text => try self.texts.append(a, try parse([]const u8, a, raw)),
                .text_origin => try self.text_entries.append(a, try parse(c.Entry, a, raw)),
            }
        }
        fn snapshot(self: Values, h: Head) Snapshot {
            return .{ .head = h, .namespace = .{ .record_store = true, .key_template = h.key_template, .revision = h.revision, .adoption_revision = h.adoption_revision, .adoption_standalone = h.adoption_standalone, .merges = self.aliases.items, .state = .{ .suppress = .{ .keys = self.suppressed.items, .entries = self.entries.items, .texts = self.texts.items, .text_entries = self.text_entries.items, .min_text_chars = h.min_text_chars }, .distinct = self.distinct.items, .redirect_origins = self.origins.items, .removed_aliases = self.removed.items } } };
        }
    };
    /// Full export is for worker configuration and explicit listing, never point reads.
    pub fn exportNamespace(self: Store, template: []const u8) !?Snapshot {
        const h = try self.head(template) orelse return null;
        var values: Values = .{};
        const prefix = try self.records(template, null);
        var after: ?[]const u8 = null;
        while (true) {
            const page = try self.backend.scan(self.backend.ptr, self.a, prefix, after, 128);
            for (page.rows) |row| {
                const tail = row.key[prefix.len..];
                const slash = std.mem.indexOfScalar(u8, tail, '/') orelse return error.InvalidCatalogRecord;
                const kind = std.meta.stringToEnum(Kind, tail[0..slash]) orelse return error.InvalidCatalogRecord;
                try values.append(self.a, kind, row.value);
            }
            after = page.next;
            if (after == null) break;
        }
        return values.snapshot(h);
    }
    pub const ForwardNode = struct { target: ?[]const u8, suppressed: bool };
    /// Minimal point state for path-compressed execution caches. Callers pin
    /// the namespace head and invalidate their cache whenever it changes.
    pub fn forwardNode(self: Store, template: []const u8, id: []const u8) !ForwardNode {
        const alias = try self.get(resolver.KeyAlias, template, .redirect, id);
        return .{ .target = if (alias) |value| value.target else null, .suppressed = try self.backend.get(self.backend.ptr, self.a, try self.record(template, .suppressed, id)) != null };
    }
    /// Forward and reverse redirect indexes bound discovery to the affected
    /// components. Distinct endpoints add only their forward canonical paths.
    pub fn forwardScope(self: Store, template: []const u8, seeds: []const []const u8) !?Snapshot {
        const h = try self.head(template) orelse return null;
        var values: Values = .{};
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (seeds) |seed| {
            var id = seed;
            while (true) {
                if ((try seen.getOrPut(self.a, id)).found_existing) break;
                if (seen.count() > 16384) return error.ResolverCurationLimitExceeded;
                inline for (.{ Kind.suppressed, Kind.suppression, Kind.redirect_origin, Kind.removed_alias }) |kind| if (try self.backend.get(self.backend.ptr, self.a, try self.record(template, kind, id))) |raw| try values.append(self.a, kind, raw);
                const alias = (try self.get(resolver.KeyAlias, template, .redirect, id)) orelse break;
                try values.aliases.append(self.a, alias);
                id = alias.target;
            }
        }
        return values.snapshot(h);
    }
    const PinReady = struct { endpoints: u64 };
    const PinCount = struct { key: []const u8, count: u64 };
    fn pinReadyPath(self: Store, template: []const u8) ![]const u8 {
        return self.keyPath(try std.fmt.allocPrint(self.a, "pin-ready/{s}", .{try hash(self.a, template)}));
    }
    fn pinCountPath(self: Store, template: []const u8, id: []const u8) ![]const u8 {
        return self.keyPath(try std.fmt.allocPrint(self.a, "pin-count/{s}/{s}", .{ try hash(self.a, template), try hash(self.a, id) }));
    }
    fn pinCount(self: Store, template: []const u8, id: []const u8) !u64 {
        const raw = try self.backend.get(self.backend.ptr, self.a, try self.pinCountPath(template, id)) orelse return 0;
        return (try parse(PinCount, self.a, raw)).count;
    }
    fn pinChild(self: Store, template: []const u8, parent: []const u8, child: []const u8, present: bool) !void {
        const path = try std.mem.concat(self.a, u8, &.{ try self.adjacent(template, "pin-child", parent), try hash(self.a, child) });
        if (present) try self.backend.put(self.backend.ptr, self.a, path, try encode(self.a, child)) else try self.backend.remove(self.backend.ptr, self.a, path);
    }
    fn setPinCount(self: Store, template: []const u8, id: []const u8, count: u64) !void {
        if (self.pin_changes) |changes| try changes.put(self.a, try self.a.dupe(u8, id), {});
        const path = try self.pinCountPath(template, id);
        if (count == 0) try self.backend.remove(self.backend.ptr, self.a, path) else try self.backend.put(self.backend.ptr, self.a, path, try encode(self.a, PinCount{ .key = id, .count = count }));
    }
    fn ownPin(self: Store, template: []const u8, id: []const u8) !bool {
        return (try self.backend.scan(self.backend.ptr, self.a, try self.adjacent(template, "distinct-key", id), null, 1)).rows.len != 0;
    }
    /// Linear, atomic adoption for old namespaces. Ordinary edits maintain the
    /// sparse index incrementally; readers never infer readiness from a head.
    pub fn ensurePinIndex(self: Store, template: []const u8) !void {
        const ready = try self.pinReadyPath(template);
        if (try self.backend.get(self.backend.ptr, self.a, ready) != null) return;
        const Node = struct { target: ?[]const u8 = null, incoming: usize = 0, pins: u64 = 0 };
        var nodes: std.StringArrayHashMapUnmanaged(Node) = .empty;
        inline for (.{ Kind.redirect, Kind.distinct }) |kind| {
            var after: ?[]const u8 = null;
            while (true) {
                const page = try self.backend.scan(self.backend.ptr, self.a, try self.records(template, kind), after, 128);
                for (page.rows) |row| {
                    if (kind == .redirect) {
                        const alias = try parse(resolver.KeyAlias, self.a, row.value);
                        const from = try nodes.getOrPut(self.a, alias.key);
                        if (!from.found_existing) from.value_ptr.* = .{};
                        from.value_ptr.target = alias.target;
                        const to = try nodes.getOrPut(self.a, alias.target);
                        if (!to.found_existing) to.value_ptr.* = .{};
                        to.value_ptr.incoming += 1;
                    } else {
                        const pair = try parse(c.Pair, self.a, row.value);
                        for ([_][]const u8{ pair.from, pair.to }) |id| {
                            const node = try nodes.getOrPut(self.a, id);
                            if (!node.found_existing) node.value_ptr.* = .{};
                            node.value_ptr.pins = 1;
                        }
                    }
                }
                after = page.next;
                if (after == null) break;
            }
        }
        var total: u64 = 0;
        for (nodes.values()) |node| total += node.pins;
        var leaves: std.ArrayList(usize) = .empty;
        for (nodes.values(), 0..) |node, i| if (node.incoming == 0) try leaves.append(self.a, i);
        var offset: usize = 0;
        while (offset < leaves.items.len) : (offset += 1) {
            const node = nodes.values()[leaves.items[offset]];
            if (node.target) |target| {
                const i = nodes.getIndex(target).?;
                nodes.values()[i].pins = try std.math.add(u64, nodes.values()[i].pins, node.pins);
                nodes.values()[i].incoming -= 1;
                if (nodes.values()[i].incoming == 0) try leaves.append(self.a, i);
            }
        }
        if (offset != nodes.count()) return error.InvalidResolverAliasCycle;
        for (nodes.keys(), nodes.values()) |id, node| if (node.pins != 0) {
            try self.setPinCount(template, id, node.pins);
            if (node.target) |target| try self.pinChild(template, target, id, true);
        };
        try self.backend.put(self.backend.ptr, self.a, ready, try encode(self.a, PinReady{ .endpoints = total }));
    }
    const PinDelta = struct { key: []const u8, amount: i128 };
    /// Union the affected ancestor paths, then propagate each aggregate once.
    /// Cuts describe a detached forest; all old source weights are captured
    /// before this pass so update order cannot multiply or double-count work.
    fn propagatePins(self: Store, template: []const u8, deltas: []const PinDelta, cuts: std.StringHashMapUnmanaged(void)) !void {
        const Node = struct { delta: i128 = 0, next: ?usize = null, incoming: usize = 0 };
        var nodes: std.StringArrayHashMapUnmanaged(Node) = .empty;
        for (deltas) |delta| {
            if (delta.amount == 0) continue;
            const entry = try nodes.getOrPut(self.a, delta.key);
            if (!entry.found_existing) entry.value_ptr.* = .{};
            entry.value_ptr.delta = try std.math.add(i128, entry.value_ptr.delta, delta.amount);
        }
        var offset: usize = 0;
        while (offset < nodes.count()) : (offset += 1) {
            const id = nodes.keys()[offset];
            if (cuts.contains(id)) continue;
            const edge = try self.get(resolver.KeyAlias, template, .redirect, id) orelse continue;
            const target = try nodes.getOrPut(self.a, edge.target);
            if (!target.found_existing) target.value_ptr.* = .{};
            target.value_ptr.incoming += 1;
            nodes.values()[offset].next = nodes.getIndex(edge.target).?;
        }
        var leaves: std.ArrayList(usize) = .empty;
        for (nodes.values(), 0..) |node, i| if (node.incoming == 0) try leaves.append(self.a, i);
        offset = 0;
        while (offset < leaves.items.len) : (offset += 1) {
            const i = leaves.items[offset];
            const node = nodes.values()[i];
            if (node.delta != 0) {
                const old = try self.pinCount(template, nodes.keys()[i]);
                const value = try std.math.add(i128, @as(i128, old), node.delta);
                const count = std.math.cast(u64, value) orelse return error.InvalidCatalogRecord;
                try self.setPinCount(template, nodes.keys()[i], count);
                if (node.next) |next| try self.pinChild(template, nodes.keys()[next], nodes.keys()[i], count != 0);
            }
            if (node.next) |next| {
                nodes.values()[next].delta = try std.math.add(i128, nodes.values()[next].delta, node.delta);
                nodes.values()[next].incoming -= 1;
                if (nodes.values()[next].incoming == 0) try leaves.append(self.a, next);
            }
        }
        if (offset != nodes.count()) return error.InvalidResolverAliasCycle;
    }
    /// Query policy is projected at one native read cut. Rendering follows only
    /// forward edges; scoring also needs incident distinct pins and their paths.
    /// Text suppression is always a point lookup, never a namespace export.
    const QueryGraph = struct { head: Stamp, fingerprint: [32]u8, bytes: u64 };
    fn queryGraphPath(self: Store, template: []const u8) ![]const u8 {
        return self.keyPath(try std.fmt.allocPrint(self.a, "query-graph/{s}", .{try hash(self.a, template)}));
    }
    fn graphKind(kind: Kind) bool {
        return kind == .redirect or kind == .removed_alias or kind == .distinct;
    }
    fn queryGraph(self: Store, h: Head) !QueryGraph {
        if (try self.backend.get(self.backend.ptr, self.a, try self.queryGraphPath(h.key_template))) |raw| {
            const graph = try parse(QueryGraph, self.a, raw);
            if (graph.head.revision == h.revision and std.mem.eql(u8, &graph.head.fingerprint, &h.fingerprint)) return graph;
        }
        // An older writer or restored legacy snapshot has no qualified graph
        // stamp. The complete lineage remains a conservative cache identity.
        return .{ .head = .{ .revision = h.revision, .fingerprint = h.fingerprint }, .fingerprint = h.fingerprint, .bytes = h.bytes };
    }
    fn putQueryGraph(self: Store, h: Head, fingerprint: [32]u8, bytes: u64) !void {
        try self.backend.put(self.backend.ptr, self.a, try self.queryGraphPath(h.key_template), try encode(self.a, QueryGraph{ .head = .{ .revision = h.revision, .fingerprint = h.fingerprint }, .fingerprint = fingerprint, .bytes = bytes }));
    }
    const RoutingStamp = struct { head: Stamp, fingerprint: [32]u8 };
    fn routingPath(self: Store, template: []const u8) ![]const u8 {
        return self.keyPath(try std.fmt.allocPrint(self.a, "query-routing/{s}", .{try hash(self.a, template)}));
    }
    fn routingFingerprint(self: Store, h: Head) ![32]u8 {
        if (try self.backend.get(self.backend.ptr, self.a, try self.routingPath(h.key_template))) |raw| {
            const stamp = try parse(RoutingStamp, self.a, raw);
            if (stamp.head.revision == h.revision and std.mem.eql(u8, &stamp.head.fingerprint, &h.fingerprint)) return stamp.fingerprint;
        }
        return h.fingerprint;
    }
    fn putRouting(self: Store, h: Head, fingerprint: [32]u8) !void {
        try self.backend.put(self.backend.ptr, self.a, try self.routingPath(h.key_template), try encode(self.a, RoutingStamp{ .head = .{ .revision = h.revision, .fingerprint = h.fingerprint }, .fingerprint = fingerprint }));
    }
    pub const QueryProfile = struct {
        id: []const u8,
        // Complete native lookups need no full-profile loader. Legacy or
        // explicitly bulk callers supply the loader and its build estimate.
        ptr: ?*anyopaque = null,
        load: ?*const fn (*anyopaque, A) anyerror![]const resolver.KeyAlias = null,
        estimated_bytes: usize = std.math.maxInt(usize),
        lookup: ?AliasLookup = null,
    };
    pub const InlineQueryProfile = struct {
        aliases: []const resolver.KeyAlias,
        pub fn estimate(self: InlineQueryProfile) usize {
            var bytes: usize = 0;
            for (self.aliases) |alias| bytes +|= 1024 +| (alias.key.len +| alias.target.len) *| 8;
            return bytes;
        }
        pub fn load(ptr: *anyopaque, a: A) ![]const resolver.KeyAlias {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return parse([]const resolver.KeyAlias, a, try encode(a, self.aliases));
        }
    };
    pub const PreparedQuery = struct {
        lease: ?query_context.Lease,
        lookup: ?AliasLookup = null,
        indexed: bool = false,
        pin_discovery: bool = true,
        profile_id: []const u8 = "",
        routing_id: [32]u8 = @splat(0),
        graph_id: [32]u8 = @splat(0),
        stamp: Stamp,
        template: []const u8,
        distinct: bool,
        pub fn release(self: PreparedQuery) void {
            if (self.lease) |lease| lease.release();
        }
    };
    /// Published native lookups keep foreground work bounded independently of
    /// cache residency. Full compilation is reserved for explicit bulk/legacy
    /// profiles without a complete lookup, at the caller's pinned read cut.
    pub fn prepareQuery(self: Store, template: []const u8, profile: QueryProfile, distinct: bool) !?PreparedQuery {
        const h = (try self.head(template)) orelse return null;
        var composed = distinct;
        if (distinct) if (try self.backend.get(self.backend.ptr, self.a, try self.pinReadyPath(template))) |raw| {
            composed = (try parse(PinReady, self.a, raw)).endpoints != 0;
        };
        const graph_state = try self.queryGraph(h);
        const static_estimate = profile.estimated_bytes *| 4;
        const native_indexed = if (profile.lookup) |lookup| lookup.node != null and lookup.seek_node != null else false;
        if (!native_indexed and profile.load == null) return error.InvalidCatalogRecord;
        const indexed = native_indexed or (composed and std.mem.allEqual(u8, &h.fingerprint, 0)) or static_estimate > query_context.Cache.max_bytes or (composed and graph_state.bytes *| 128 > query_context.Cache.max_bytes / 2);
        var result: PreparedQuery = .{ .lease = null, .indexed = indexed, .pin_discovery = composed, .profile_id = profile.id, .routing_id = try self.routingFingerprint(h), .graph_id = graph_state.fingerprint, .stamp = .{ .revision = h.revision, .fingerprint = h.fingerprint }, .template = template, .distinct = distinct };
        if (indexed and profile.lookup != null and profile.lookup.?.seek_node != null) {
            result.lookup = profile.lookup;
            return result;
        }
        const BaseLoader = struct {
            profile: QueryProfile,
            fn load(ptr: *anyopaque, a: A) !query_context.Graph {
                const loader: *@This() = @ptrCast(@alignCast(ptr));
                if (comptime @import("builtin").is_test) _ = test_query_base_loads.fetchAdd(1, .monotonic);
                const callback = loader.profile.load orelse return error.InvalidCatalogRecord;
                const ptr_ = loader.profile.ptr orelse return error.InvalidCatalogRecord;
                return .{ .aliases = try callback(ptr_, a), .shared_profile = true };
            }
        };
        var base_loader: BaseLoader = .{ .profile = profile };
        var key: [32]u8 = undefined;
        @import("antfly_hash").Sha256.hash(try encode(self.a, .{ template, profile.id, @as(?[32]u8, null) }), &key, .{});
        const base = (try query_context.tryAcquire(key, true, .{ .ptr = &base_loader, .load = BaseLoader.load, .estimated_bytes = static_estimate +| 4096 })) orelse legacy: {
            if (profile.lookup != null and profile.lookup.?.seek_node != null) {
                result.indexed = true;
                result.lookup = profile.lookup;
                return result;
            }
            // Legacy inline profiles lack a persistent node projection. They
            // still need their immutable static index to answer exactly.
            break :legacy try query_context.acquire(key, true, .{ .ptr = &base_loader, .load = BaseLoader.load });
        };
        errdefer base.release();
        result.lease = base;
        if (!composed or indexed) return result;
        const Loader = struct {
            store: Store,
            base: query_context.Lease,
            template: []const u8,
            fn load(ptr: *anyopaque, a: A) !query_context.Graph {
                const loader: *@This() = @ptrCast(@alignCast(ptr));
                var store = loader.store;
                store.a = a;
                var values: Values = .{};
                inline for (.{ Kind.redirect, Kind.removed_alias, Kind.distinct }) |kind| {
                    const prefix = try store.records(loader.template, kind);
                    var after: ?[]const u8 = null;
                    while (true) {
                        const page = try store.backend.scan(store.backend.ptr, a, prefix, after, 128);
                        for (page.rows) |row| try values.append(a, kind, row.value);
                        after = page.next;
                        if (after == null) break;
                    }
                }
                return .{ .base = loader.base, .composed = true, .native_aliases = values.aliases.items, .removed = values.removed.items, .pairs = values.distinct.items };
            }
        };
        var loader: Loader = .{ .store = self, .base = base, .template = template };
        @import("antfly_hash").Sha256.hash(try encode(self.a, .{ template, profile.id, @as(?[32]u8, graph_state.fingerprint) }), &key, .{});
        // Only overlay data and the potentially affected static frontier need
        // build room. The immutable profile is already charged by its lease.
        const estimate = (graph_state.bytes *| 128) +| (base.context.nodes.nodes.len *| 512) +| 4096;
        const overlay = (try query_context.tryAcquire(key, true, .{ .ptr = &loader, .load = Loader.load, .estimated_bytes = @intCast(estimate) })) orelse {
            result.indexed = true;
            if (profile.lookup != null and profile.lookup.?.seek_node != null) {
                base.release();
                result.lease = null;
                result.lookup = profile.lookup;
            }
            return result;
        };
        base.release();
        result.lease = overlay;
        return result;
    }
    fn appendTextPolicy(self: Store, template: []const u8, texts: []const []const u8, snapshot: *Snapshot) !void {
        var rules: std.ArrayList([]const u8) = .empty;
        var normalized: std.StringHashMapUnmanaged(void) = .empty;
        for (texts) |text| {
            const key = try c.normalize(self.a, text);
            if ((try normalized.getOrPut(self.a, key)).found_existing) continue;
            if (try self.get([]const u8, template, .text, key)) |rule| try rules.append(self.a, rule);
        }
        snapshot.namespace.state.suppress.texts = rules.items;
    }
    pub fn textPolicy(self: Store, template: []const u8, texts: []const []const u8) !?Snapshot {
        const h = (try self.head(template)) orelse return null;
        var snapshot = (Values{}).snapshot(h);
        try self.appendTextPolicy(template, texts, &snapshot);
        return snapshot;
    }
    const StaticQueryLookup = struct {
        context: ?*const query_context.Context,
        fn node(ptr: *anyopaque, _: A, id: []const u8) !?query_context.QueryNode {
            const self_: *@This() = @ptrCast(@alignCast(ptr));
            return self_.context.?.nodes.by_key.get(id);
        }
        fn seekNode(ptr: *anyopaque, _: A, digest: []const u8, canonical: ?[]const u8) !?query_context.QueryNode {
            const self_: *@This() = @ptrCast(@alignCast(ptr));
            return self_.context.?.nodes.seek(digest, canonical);
        }
        fn expand(ptr: *anyopaque, a: A, id: []const u8, forward_only: bool, mask: EdgeMask) ![]const resolver.KeyAlias {
            const self_: *@This() = @ptrCast(@alignCast(ptr));
            const context = self_.context.?;
            var result: std.ArrayList(resolver.KeyAlias) = .empty;
            if (context.static_targets.get(id)) |target| if (try mask.enabled(mask.ptr, a, id)) try result.append(a, .{ .key = id, .target = target });
            if (!forward_only) if (context.static_reverse.get(id)) |edges| for (edges.items) |edge| {
                if (try mask.enabled(mask.ptr, a, edge.key)) try result.append(a, edge);
            };
            return result.items;
        }
    };
    fn queryForwardIndexed(self: Store, template: []const u8, seeds: []const []const u8, lookup: AliasLookup, include_configured: bool) !?Snapshot {
        const h = (try self.head(template)) orelse return null;
        var values: Values = .{};
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        const Allow = struct {
            fn enabled(_: *anyopaque, _: A, _: []const u8) !bool {
                return true;
            }
        };
        var unused: u8 = 0;
        for (seeds) |seed| {
            var id = seed;
            while (true) {
                if ((try seen.getOrPut(self.a, id)).found_existing) break;
                if (seen.count() > 16384) return error.ResolverCurationLimitExceeded;
                inline for (.{ Kind.suppressed, Kind.suppression, Kind.redirect_origin, Kind.removed_alias }) |kind| if (try self.backend.get(self.backend.ptr, self.a, try self.record(template, kind, id))) |raw| try values.append(self.a, kind, raw);
                if (try self.get(resolver.KeyAlias, template, .redirect, id)) |edge| {
                    try values.aliases.append(self.a, edge);
                    id = edge.target;
                    continue;
                }
                if (try self.get([]const u8, template, .removed_alias, id) != null) break;
                const edges = try lookup.expand(lookup.ptr, self.a, id, true, .{ .ptr = &unused, .enabled = Allow.enabled });
                if (edges.len == 0) break;
                if (edges.len != 1 or !std.mem.eql(u8, edges[0].key, id)) return error.InvalidResolverAlias;
                if (include_configured) try values.aliases.append(self.a, edges[0]);
                id = edges[0].target;
            }
        }
        var snapshot = values.snapshot(h);
        snapshot.complete_aliases = include_configured;
        return snapshot;
    }
    const QueryCanonical = struct {
        store: Store,
        template: []const u8,
        lookup: AliasLookup,
        static_only: bool,
        memo: std.AutoHashMapUnmanaged([32]u8, []const u8) = .empty,
        memo_buffer: ?*std.heap.FixedBufferAllocator = null,
        frame: ?*std.heap.FixedBufferAllocator = null,
        fn remember(self: *@This(), digests: []const [32]u8, root: []const u8) !void {
            if (self.memo_buffer) |buffer| {
                const a = buffer.allocator();
                const owned_root = a.dupe(u8, root) catch return;
                for (digests) |digest| self.memo.put(a, digest, owned_root) catch return;
            } else for (digests) |digest| try self.memo.put(self.store.a, digest, root);
        }
        fn canonical(self: *@This(), seed: []const u8) ![]const u8 {
            try c.validKey(seed);
            var seed_digest: [32]u8 = undefined;
            @import("antfly_hash").Sha256.hash(seed, &seed_digest, .{});
            if (self.memo.get(seed_digest)) |known| return known;
            const a = self.store.a;
            if (self.static_only) {
                const node = try self.lookup.node.?(self.lookup.ptr, a, seed);
                const root = if (node) |value| value.canonical else seed;
                try self.remember(&.{seed_digest}, root);
                return root;
            }
            // Short paths stay on the stack. Longer paths borrow the unused
            // tail of the caller's frame, or temporary service scratch. Neither
            // decoded JSON nor full key strings accumulate with hop count.
            var short: [64][32]u8 = undefined;
            var path: [][32]u8 = &short;
            var slots: ?[]u32 = null;
            var count: usize = 0;
            var owned: ?query_context.RoutingScratch = null;
            defer if (owned) |scratch| scratch.deinit();
            var original_buffer: ?[]u8 = null;
            defer if (original_buffer) |buffer| {
                self.frame.?.buffer = buffer;
            };
            var io_bytes: [256 * 1024]u8 = undefined;
            var io: std.heap.FixedBufferAllocator = .init(&io_bytes);
            var temporary = self.store;
            temporary.a = io.allocator();
            var current: [16384]u8 = undefined;
            var len = seed.len;
            @memcpy(current[0..len], seed);
            const terminal = walk: while (true) {
                var digest: [32]u8 = undefined;
                @import("antfly_hash").Sha256.hash(current[0..len], &digest, .{});
                if (self.memo.get(digest)) |known| break :walk known;
                if (count == short.len and slots == null) {
                    const capacity = 640 * 1024;
                    const storage = if (self.frame != null and self.frame.?.buffer.len - self.frame.?.end_index >= capacity + 32768) blk: {
                        const buffer = self.frame.?.buffer;
                        original_buffer = buffer;
                        self.frame.?.buffer = buffer[0 .. buffer.len - capacity];
                        break :blk buffer[buffer.len - capacity ..];
                    } else blk: {
                        owned = try query_context.RoutingScratch.init();
                        break :blk owned.?.bytes[0..capacity];
                    };
                    var workspace: std.heap.FixedBufferAllocator = .init(storage);
                    path = try workspace.allocator().alloc([32]u8, 16384);
                    @memcpy(path[0..count], &short);
                    slots = try workspace.allocator().alloc(u32, 32768);
                    @memset(slots.?, 0);
                    for (path[0..count], 0..) |seen, i| {
                        var slot: usize = @intCast(std.mem.readInt(u64, seen[0..8], .little) & 32767);
                        while (slots.?[slot] != 0) slot = (slot + 1) & 32767;
                        slots.?[slot] = @intCast(i + 1);
                    }
                }
                if (slots) |table| {
                    var slot: usize = @intCast(std.mem.readInt(u64, digest[0..8], .little) & 32767);
                    while (table[slot] != 0) : (slot = (slot + 1) & 32767) if (std.mem.eql(u8, &path[table[slot] - 1], &digest)) return error.InvalidResolverAliasCycle;
                    if (count == path.len) return error.ResolverCurationLimitExceeded;
                    table[slot] = @intCast(count + 1);
                } else for (path[0..count]) |seen| if (std.mem.eql(u8, &seen, &digest)) return error.InvalidResolverAliasCycle;
                path[count] = digest;
                count += 1;
                io.reset();
                const next = (try temporary.queryPinNext(self.template, self.lookup, current[0..len])) orelse break :walk current[0..len];
                try c.validKey(next);
                len = next.len;
                @memcpy(current[0..len], next);
            };
            const root = try a.dupe(u8, terminal);
            try self.remember(path[0..count], root);
            return root;
        }
    };
    fn queryPinNodes(self: Store, template: []const u8, component_scope: ?[]const u8, canonical: *QueryCanonical, roots: std.StringHashMapUnmanaged(void), starts: *std.StringHashMapUnmanaged(void), membership: ?*RoutingBuild) !void {
        const prefix = try self.keyPath(try std.fmt.allocPrint(self.a, "pin-count/{s}/", .{try hash(self.a, template)}));
        var digest: [64]u8 = @splat('0');
        while (true) {
            // Only selected/frozen membership entries outlive each posting.
            // Cursor and path scratch are reused for every posting.
            if (canonical.frame) |frame| {
                frame.reset();
                if (canonical.memo_buffer) |buffer| {
                    if (buffer.end_index > buffer.buffer.len / 2) {
                        buffer.reset();
                        canonical.memo = .empty;
                    }
                } else canonical.memo = .empty;
            }
            const a = canonical.store.a;
            const node = (try canonical.lookup.seek_node.?(canonical.lookup.ptr, a, &digest, component_scope)) orelse return;
            var node_digest: [64]u8 = undefined;
            @memcpy(&node_digest, try hash(a, node.key));
            const before = try std.mem.concat(a, u8, &.{ prefix, &node_digest });
            before[before.len - 1] -= 1;
            const page = try self.backend.scan(self.backend.ptr, a, prefix, before, 1);
            if (page.rows.len == 0) return;
            const row = page.rows[0];
            if (!std.mem.startsWith(u8, row.key, prefix) or row.key.len != prefix.len + 64) return error.InvalidCatalogRecord;
            const posting_digest = row.key[prefix.len..];
            if (!std.mem.eql(u8, posting_digest, &node_digest)) {
                @memcpy(&digest, posting_digest);
                continue;
            }
            const posting = try parse(PinCount, a, row.value);
            if (posting.count == 0 or !std.mem.eql(u8, posting.key, node.key)) return error.InvalidCatalogRecord;
            const root = if (canonical.static_only) node.canonical else try canonical.canonical(node.key);
            if (membership) |builder| try builder.add(root, node.key) else if (roots.contains(root)) try starts.put(self.a, try self.a.dupe(u8, node.key), {});
            digest = node_digest;
            digest[digest.len - 1] += 1;
        }
    }
    const RoutingBuild = struct {
        a: A,
        seen: std.StringHashMapUnmanaged(void) = .empty,
        routes: std.ArrayList(query_context.Route) = .empty,
        bytes: usize = 0,
        fn add(self: *@This(), root: []const u8, component_scope: []const u8) !void {
            if ((try self.seen.getOrPut(self.a, try encode(self.a, .{ root, component_scope }))).found_existing) return;
            self.bytes +|= @sizeOf(query_context.Route) +| root.len +| component_scope.len;
            if (self.bytes > 1024 * 1024) return error.OutOfMemory;
            try self.routes.append(self.a, .{ .root = try self.a.dupe(u8, root), .component = try self.a.dupe(u8, component_scope) });
        }
    };
    fn freezeRoutes(a: A, builder: RoutingBuild) !query_context.Graph {
        const items = try a.alloc(query_context.Route, builder.routes.items.len);
        for (items, builder.routes.items) |*route, value| route.* = .{ .root = try a.dupe(u8, value.root), .component = try a.dupe(u8, value.component) };
        std.mem.sort(query_context.Route, items, {}, struct {
            fn less(_: void, left: query_context.Route, right: query_context.Route) bool {
                const order = std.mem.order(u8, left.root, right.root);
                return if (order == .eq) std.mem.lessThan(u8, left.component, right.component) else order == .lt;
            }
        }.less);
        return .{ .routes = items };
    }
    fn queryPinMembership(self: Store, prepared: PreparedQuery, lookup: AliasLookup, changes: bool) !?query_context.Lease {
        if (prepared.profile_id.len == 0 or std.mem.allEqual(u8, &prepared.stamp.fingerprint, 0)) return null;
        var key: [32]u8 = undefined;
        @import("antfly_hash").Sha256.hash(try encode(self.a, .{ "pin-membership-v2", prepared.template, prepared.profile_id, prepared.graph_id }), &key, .{});
        const Loader = struct {
            store: Store,
            template: []const u8,
            lookup: AliasLookup,
            changes: bool,
            fn load(ptr: *anyopaque, a: A) !query_context.Graph {
                const loader: *@This() = @ptrCast(@alignCast(ptr));
                const scratch = try query_context.RoutingScratch.init();
                defer scratch.deinit();
                var bounded: std.heap.FixedBufferAllocator = .init(scratch.bytes[0 .. 4 * 1024 * 1024]);
                var frame: std.heap.FixedBufferAllocator = .init(scratch.bytes[4 * 1024 * 1024 .. 6 * 1024 * 1024]);
                var memo: std.heap.FixedBufferAllocator = .init(scratch.bytes[6 * 1024 * 1024 ..]);
                var store = loader.store;
                store.a = bounded.allocator();
                var temporary = store;
                temporary.a = frame.allocator();
                var canonical: QueryCanonical = .{ .store = temporary, .template = loader.template, .lookup = loader.lookup, .static_only = !loader.changes, .frame = &frame, .memo_buffer = &memo };
                var builder: RoutingBuild = .{ .a = store.a };
                var ignored: std.StringHashMapUnmanaged(void) = .empty;
                try store.queryPinNodes(loader.template, null, &canonical, .empty, &ignored, &builder);
                return freezeRoutes(a, builder);
            }
        };
        var loader: Loader = .{ .store = self, .template = prepared.template, .lookup = lookup, .changes = changes };
        return query_context.tryRouting(key, .{ .ptr = &loader, .load = Loader.load }) catch |err| switch (err) {
            error.OutOfMemory => null,
            else => return err,
        };
    }
    const PinProjectionGC = struct { epoch: u64, after: ?[]const u8 = null, complete: bool = false };
    pub fn queryPinsCleanupCurrent(self: Store, template: []const u8, lookup: AliasLookup) !bool {
        const epoch_fn = lookup.profile_epoch orelse return true;
        if (try self.head(template) == null) return true;
        const path = try self.keyPath(try std.fmt.allocPrint(self.a, "query-pins-gc-v1/{s}", .{try hash(self.a, template)}));
        const raw = (try self.backend.get(self.backend.ptr, self.a, path)) orelse return false;
        const state = try parse(PinProjectionGC, self.a, raw);
        return state.complete and state.epoch == try epoch_fn(lookup.ptr, self.a);
    }
    /// Immutable profile enrollment epochs let bounded owner maintenance
    /// reclaim retired profiles without a directory scan on foreground reads.
    fn pruneQueryPins(self: Store, template: []const u8, lookup: AliasLookup, limit: u32, progress: *bool) !bool {
        const active = lookup.profile_active orelse return true;
        const epoch_fn = lookup.profile_epoch orelse return true;
        const epoch = try epoch_fn(lookup.ptr, self.a);
        const namespace = try hash(self.a, template);
        const prefix = try self.keyPath(try std.fmt.allocPrint(self.a, "query-pins-v1/{s}/", .{namespace}));
        const state_path = try self.keyPath(try std.fmt.allocPrint(self.a, "query-pins-gc-v1/{s}", .{namespace}));
        const raw = try self.backend.get(self.backend.ptr, self.a, state_path);
        var state: PinProjectionGC = if (raw) |value| try parse(PinProjectionGC, self.a, value) else .{ .epoch = epoch };
        if (state.epoch != epoch) state = .{ .epoch = epoch };
        if (state.complete) return true;
        var remaining = limit;
        while (remaining > 2) {
            const page = try self.backend.scan(self.backend.ptr, self.a, prefix, if (state.after) |after| try std.mem.concat(self.a, u8, &.{ prefix, after }) else null, 1);
            if (page.rows.len == 0) {
                state.complete = true;
                state.after = null;
                break;
            }
            const relative = page.rows[0].key[prefix.len..];
            if (relative.len < 65 or relative[64] != '/') return error.InvalidCatalogRecord;
            const profile = relative[0..64];
            if (try active(lookup.ptr, self.a, profile)) {
                state.after = try std.mem.concat(self.a, u8, &.{ profile, "~" });
                remaining -= 1;
                continue;
            }
            const retired = try std.fmt.allocPrint(self.a, "{s}{s}/", .{ prefix, profile });
            // Remove readiness before any member page, even if an immutable
            // profile is re-enrolled while cleanup is paused between turns.
            try self.backend.remove(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ retired, "state" }));
            remaining -= 1;
            const obsolete = try self.backend.scan(self.backend.ptr, self.a, retired, null, remaining - 1);
            for (obsolete.rows) |row| try self.backend.remove(self.backend.ptr, self.a, row.key);
            remaining -= @intCast(obsolete.rows.len);
            state.after = if (obsolete.next == null) try std.mem.concat(self.a, u8, &.{ profile, "~" }) else try self.a.dupe(u8, profile);
            if (obsolete.next != null) break;
        }
        try self.backend.put(self.backend.ptr, self.a, state_path, try encode(self.a, state));
        progress.* = true;
        return state.complete;
    }
    const PinEditHead = struct { sequence: u64 = 0, graph: [32]u8, indexed: bool = false, barrier: u64 = 0, compact: bool = false, tails_indexed: bool = false, routing_indexed: bool = false, routing_origin: ?[32]u8 = null };
    const PinInvalidation = struct { sequence: u64, key: []const u8 };
    const PinEdit = struct { sequence: u64, before: [32]u8, graph: [32]u8, boundaries: []const []const u8 = &.{}, routing: []const []const u8 = &.{}, targets: []const []const u8 = &.{}, chained_targets: []const []const u8 = &.{}, invalidations: []const []const u8 = &.{}, rebuild: bool = false };
    fn pinEditRoot(self: Store, template: []const u8) ![]const u8 {
        return self.keyPath(try std.fmt.allocPrint(self.a, "pin-edits-v1/{s}/", .{try hash(self.a, template)}));
    }
    fn pinEditSlot(self: Store, template: []const u8, sequence: u64) ![]const u8 {
        return std.fmt.allocPrint(self.a, "{s}slot/{d:0>3}", .{ try self.pinEditRoot(template), sequence % 256 });
    }
    fn pinEditHead(self: Store, template: []const u8, graph: [32]u8) !PinEditHead {
        const path = try std.mem.concat(self.a, u8, &.{ try self.pinEditRoot(template), "head" });
        return if (try self.backend.get(self.backend.ptr, self.a, path)) |raw| try parse(PinEditHead, self.a, raw) else .{ .graph = graph };
    }
    fn invalidationPath(self: Store, template: []const u8, sequence: u64, digest: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "{s}invalidated/{d:0>20}/{s}", .{ try self.pinEditRoot(template), sequence, digest });
    }
    fn appendPinEdit(self: Store, template: []const u8, before: [32]u8, graph: [32]u8, boundaries: []const []const u8, routing: []const []const u8, targets: []const []const u8) !void {
        const journal = try self.pinEditRoot(template);
        const head_path = try std.mem.concat(self.a, u8, &.{ journal, "head" });
        const raw = (try self.backend.get(self.backend.ptr, self.a, head_path)) orelse return;
        const old = try parse(PinEditHead, self.a, raw);
        if (!std.mem.eql(u8, &old.graph, &before)) return error.InvalidCatalogRecord;
        const sequence = try std.math.add(u64, old.sequence, 1);
        var affected: std.StringArrayHashMapUnmanaged(void) = .empty;
        for ([_][]const []const u8{ boundaries, routing, targets }) |keys| for (keys) |id| try affected.put(self.a, id, {});
        const rebuild = boundaries.len > 256 or routing.len > 128 or targets.len > 256 or affected.count() > 640;
        var edit: PinEdit = .{ .sequence = sequence, .before = before, .graph = graph, .boundaries = if (rebuild) &.{} else boundaries, .routing = if (rebuild) &.{} else routing, .targets = if (rebuild) &.{} else targets, .invalidations = if (rebuild) &.{} else affected.keys(), .rebuild = rebuild };
        // Capture native tail entry points, not their transitive closure. The
        // deduplicated destination index lets readers check newly reachable
        // native chains without materializing them for every static profile.
        if (!edit.rebuild) {
            var tail_bytes: [128 * 1024]u8 = undefined;
            var tail_frame: std.heap.FixedBufferAllocator = .init(&tail_bytes);
            var temporary = self;
            temporary.a = tail_frame.allocator();
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            var tails: std.ArrayList([]const u8) = .empty;
            for (edit.targets) |id| {
                if ((try seen.getOrPut(self.a, id)).found_existing) continue;
                tail_frame.reset();
                const value = self.backend.get(self.backend.ptr, temporary.a, try temporary.record(template, .redirect, id)) catch |err| {
                    if (err != error.OutOfMemory) return err;
                    edit.rebuild = true;
                    break;
                };
                if (value != null) try tails.append(self.a, id);
            }
            edit.chained_targets = tails.items;
        }
        if (edit.rebuild) {
            edit.boundaries = &.{};
            edit.routing = &.{};
            edit.targets = &.{};
            edit.chained_targets = &.{};
            edit.invalidations = &.{};
        }
        var bytes: [64 * 1024]u8 = undefined;
        var bounded: std.heap.FixedBufferAllocator = .init(&bytes);
        const payload = encode(bounded.allocator(), edit) catch |err| blk: {
            if (err != error.OutOfMemory) return err;
            edit.rebuild = true;
            edit.boundaries = &.{};
            edit.routing = &.{};
            edit.targets = &.{};
            edit.chained_targets = &.{};
            edit.invalidations = &.{};
            break :blk try encode(self.a, edit);
        };
        // Deduplicate by key, ordered by the latest mutation sequence. Evict
        // only the version still owned by the expired ring event. Space is
        // bounded by retained history, without a scan or per-profile fanout.
        const slot = try self.pinEditSlot(template, sequence);
        if (try self.backend.get(self.backend.ptr, self.a, slot)) |expired_raw| {
            const expired = try parse(PinEdit, self.a, expired_raw);
            for (expired.chained_targets) |id| {
                const digest = try hash(self.a, id);
                const key = try std.fmt.allocPrint(self.a, "{s}tail-key/{s}", .{ journal, digest });
                if (try self.backend.get(self.backend.ptr, self.a, key)) |value| if (try parse(u64, self.a, value) == expired.sequence) {
                    try self.backend.remove(self.backend.ptr, self.a, key);
                    try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}tails/{d:0>20}/{s}", .{ journal, expired.sequence, digest }));
                };
            }
            for (expired.routing) |id| {
                const digest = try hash(self.a, id);
                const key = try std.fmt.allocPrint(self.a, "{s}routed-key/{s}", .{ journal, digest });
                if (try self.backend.get(self.backend.ptr, self.a, key)) |value| if (try parse(u64, self.a, value) == expired.sequence) {
                    try self.backend.remove(self.backend.ptr, self.a, key);
                    try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}routed/{d:0>20}/{s}", .{ journal, expired.sequence, digest }));
                };
            }
            for (expired.invalidations) |id| {
                const digest = try hash(self.a, id);
                const key = try std.fmt.allocPrint(self.a, "{s}invalid-key/{s}", .{ journal, digest });
                if (try self.backend.get(self.backend.ptr, self.a, key)) |value| if (try parse(u64, self.a, value) == expired.sequence) {
                    try self.backend.remove(self.backend.ptr, self.a, key);
                    try self.backend.remove(self.backend.ptr, self.a, try self.invalidationPath(template, expired.sequence, digest));
                };
            }
        }
        for (edit.invalidations) |id| {
            const digest = try hash(self.a, id);
            const key = try std.fmt.allocPrint(self.a, "{s}invalid-key/{s}", .{ journal, digest });
            if (try self.backend.get(self.backend.ptr, self.a, key)) |previous| try self.backend.remove(self.backend.ptr, self.a, try self.invalidationPath(template, try parse(u64, self.a, previous), digest));
            try self.backend.put(self.backend.ptr, self.a, key, try encode(self.a, sequence));
            try self.backend.put(self.backend.ptr, self.a, try self.invalidationPath(template, sequence, digest), try encode(self.a, PinInvalidation{ .sequence = sequence, .key = id }));
        }
        for (edit.chained_targets) |id| {
            const digest = try hash(self.a, id);
            const key = try std.fmt.allocPrint(self.a, "{s}tail-key/{s}", .{ journal, digest });
            if (try self.backend.get(self.backend.ptr, self.a, key)) |old_sequence| try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}tails/{d:0>20}/{s}", .{ journal, try parse(u64, self.a, old_sequence), digest }));
            try self.backend.put(self.backend.ptr, self.a, key, try encode(self.a, sequence));
            try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}tails/{d:0>20}/{s}", .{ journal, sequence, digest }), try encode(self.a, PinInvalidation{ .sequence = sequence, .key = id }));
        }
        var routed_seen: std.StringHashMapUnmanaged(void) = .empty;
        for (edit.routing) |id| {
            if ((try routed_seen.getOrPut(self.a, id)).found_existing) continue;
            const digest = try hash(self.a, id);
            const key = try std.fmt.allocPrint(self.a, "{s}routed-key/{s}", .{ journal, digest });
            if (try self.backend.get(self.backend.ptr, self.a, key)) |prior| try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}routed/{d:0>20}/{s}", .{ journal, try parse(u64, self.a, prior), digest }));
            try self.backend.put(self.backend.ptr, self.a, key, try encode(self.a, sequence));
            try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}routed/{d:0>20}/{s}", .{ journal, sequence, digest }), try encode(self.a, PinInvalidation{ .sequence = sequence, .key = id }));
        }
        const routed_compact = try self.a.alloc(u8, 8 + edit.routing.len * 32);
        std.mem.writeInt(u64, routed_compact[0..8], sequence, .little);
        for (edit.routing, 0..) |id, i| @import("antfly_hash").Sha256.hash(id, routed_compact[8 + i * 32 ..][0..32], .{});
        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}routed-compact/{d:0>3}", .{ journal, sequence % 256 }), routed_compact);
        const compact = try self.a.alloc(u8, 8 + edit.invalidations.len * 32);
        std.mem.writeInt(u64, compact[0..8], sequence, .little);
        for (edit.invalidations, 0..) |id, index| @import("antfly_hash").Sha256.hash(id, compact[8 + index * 32 ..][0..32], .{});
        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}compact/{d:0>3}", .{ journal, sequence % 256 }), compact);
        try self.backend.put(self.backend.ptr, self.a, slot, try self.a.dupe(u8, payload));
        try self.backend.put(self.backend.ptr, self.a, head_path, try encode(self.a, PinEditHead{ .sequence = sequence, .graph = graph, .indexed = true, .compact = true, .tails_indexed = true, .routing_indexed = true, .routing_origin = old.routing_origin orelse before, .barrier = if (edit.rebuild or !old.indexed or !old.compact or !old.tails_indexed or !old.routing_indexed or old.routing_origin == null) sequence else old.barrier }));
    }
    fn pinEdit(self: Store, template: []const u8, sequence: u64) !?PinEdit {
        const raw = (try self.backend.get(self.backend.ptr, self.a, try self.pinEditSlot(template, sequence))) orelse return null;
        const edit = try parse(PinEdit, self.a, raw);
        return if (edit.sequence == sequence and !edit.rebuild) edit else null;
    }
    const PinRepair = struct {
        stage: enum { seed, requeue, expand, paths, nodes, guard_expand, guards, members, retire, prune, cleanup } = .seed,
        sequence: u64,
        index: usize = 0,
        after: ?[]const u8 = null,
        root: ?[]const u8 = null,
        cursor: ?[]const u8 = null,
        // Retained so a format-2 repair cursor can be decoded before rebuilding.
        routing: bool = false,
        target: u64,
        ingest_graph: ?[32]u8 = null,
    };
    pub const pin_projection_format: u8 = 5;
    pub const PinProjection = struct {
        graph: [32]u8,
        phase: enum { clear, nodes, guards, build, ready, repair } = .clear,
        format: u8 = 0,
        sequence: ?u64 = null,
        static_only: bool = false,
        // Legacy format-2 field; dependency coverage replaces this heuristic.
        static_base: bool = false,
        nodes_stage: enum { configured, redirects, tombstones } = .configured,
        trace_clean: bool = false,
        repair: ?PinRepair = null,
        after: ?[]const u8 = null,
        walk: ?PinWalk = null,
    };
    const PinWalk = struct { key: []const u8, current: []const u8, hops: u32 = 0, root: ?[]const u8 = null, invalid: bool = false };
    const PinPath = struct { key: []const u8, root: []const u8, next: ?[]const u8 = null };
    const PinMember = struct { root: []const u8, key: []const u8 };
    const PinGuard = struct { key: []const u8, parent: []const u8, root: []const u8, latent: bool };
    const GuardEdge = struct { key: []const u8, parent: []const u8 };
    const DirtyPinNode = struct { key: []const u8, root: ?[]const u8 = null };
    fn pinFilter(self: Store, root: []const u8) dependency_filter.Index {
        return .{ .a = self.a, .backend = self.backend, .root = root };
    }
    fn pinFilterUpdate(self: Store, root: []const u8, terminal: []const u8, key: []const u8, add: bool) !void {
        var digest: [32]u8 = undefined;
        @import("antfly_hash").Sha256.hash(key, &digest, .{});
        try self.pinFilter(root).update(try hash(self.a, terminal), &digest, add);
    }
    fn tracePinNode(self: Store, root: []const u8, key: []const u8) !void {
        const path = try std.fmt.allocPrint(self.a, "{s}trace/{s}", .{ root, try hash(self.a, key) });
        if (try self.backend.get(self.backend.ptr, self.a, path) == null) try self.backend.put(self.backend.ptr, self.a, path, try encode(self.a, key));
    }
    fn cleanPinTrace(self: Store, root: []const u8, state: *PinProjection, budget: *u32) !void {
        if (!state.trace_clean) return;
        const page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "trace/" }), null, budget.*);
        for (page.rows) |row| try self.backend.remove(self.backend.ptr, self.a, row.key);
        budget.* -= @intCast(page.rows.len);
        if (page.next == null) state.trace_clean = false;
    }
    fn putPinPath(self: Store, root: []const u8, value: PinPath) !void {
        try self.pinFilterUpdate(root, value.root, value.key, true);
        if (value.next) |next| try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}path-in/{s}/{s}", .{ root, try hash(self.a, next), try hash(self.a, value.key) }), try encode(self.a, value));
        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}path/{s}", .{ root, try hash(self.a, value.key) }), try encode(self.a, value));
        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}root-path/{s}/{s}", .{ root, try hash(self.a, value.root), try hash(self.a, value.key) }), try encode(self.a, value));
    }
    fn putPinMember(self: Store, root: []const u8, value: PinMember) !void {
        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}member/{s}/{s}", .{ root, try hash(self.a, value.root), try hash(self.a, value.key) }), try encode(self.a, value));
        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}boundary/{s}", .{ root, try hash(self.a, value.key) }), try encode(self.a, value));
    }
    fn repairItem(self: Store, root: []const u8, kind: []const u8, id: []const u8) !void {
        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}dirty-{s}/{s}", .{ root, kind, try hash(self.a, id) }), try encode(self.a, id));
    }
    fn pinProjectionRoot(self: Store, template: []const u8, profile: []const u8) ![]const u8 {
        if (profile.len != 64) return error.InvalidResolverCurationRequest;
        for (profile) |byte| if (!std.ascii.isHex(byte)) return error.InvalidResolverCurationRequest;
        return self.keyPath(try std.fmt.allocPrint(self.a, "query-pins-v1/{s}/{s}/", .{ try hash(self.a, template), profile }));
    }
    fn hasQueryOverrides(self: Store, template: []const u8) !bool {
        return (try self.backend.scan(self.backend.ptr, self.a, try self.records(template, .redirect), null, 1)).rows.len != 0 or
            (try self.backend.scan(self.backend.ptr, self.a, try self.records(template, .removed_alias), null, 1)).rows.len != 0;
    }
    pub fn queryPinsIndexed(self: Store, template: []const u8, profile: []const u8) !bool {
        const h = (try self.head(template)) orelse return true;
        if (try self.backend.get(self.backend.ptr, self.a, try self.pinReadyPath(template))) |raw| if ((try parse(PinReady, self.a, raw)).endpoints == 0) return true;
        const path = try std.mem.concat(self.a, u8, &.{ try self.pinProjectionRoot(template, profile), "state" });
        const raw = (try self.backend.get(self.backend.ptr, self.a, path)) orelse return false;
        const state = try parse(PinProjection, self.a, raw);
        return state.format == pin_projection_format and state.phase == .ready and std.mem.eql(u8, &state.graph, &(try self.queryGraph(h)).fingerprint);
    }
    /// Owner work only. A turn clears or builds a bounded page in the caller's
    /// native transaction. Immutable profile identity and exact graph lineage
    /// fence the ready marker; partial and superseded generations are unreadable.
    fn pinEditsAvailable(self: Store, template: []const u8, state: PinProjection, latest: PinEditHead) !bool {
        const sequence = state.sequence orelse return false;
        if (!latest.indexed or !latest.compact or !latest.tails_indexed or !latest.routing_indexed or latest.routing_origin == null or latest.barrier > sequence or latest.sequence < sequence or latest.sequence - sequence > 256) return false;
        if (latest.sequence == sequence) return std.mem.eql(u8, &state.graph, &latest.graph);
        const first = (try self.pinEdit(template, sequence + 1)) orelse return false;
        return std.mem.eql(u8, &first.before, &state.graph);
    }
    fn seedPinBoundary(self: Store, root: []const u8, id: []const u8) !void {
        try self.repairItem(root, "boundary", id);
        const path = try std.fmt.allocPrint(self.a, "{s}boundary/{s}", .{ root, try hash(self.a, id) });
        if (try self.backend.get(self.backend.ptr, self.a, path)) |raw| {
            const old = try parse(PinMember, self.a, raw);
            try self.repairItem(root, "root", old.root);
        }
    }
    /// Advance exactly one boundary step. The caller owns a single shared
    /// hop/write budget and persists the constant-size walk between turns.
    fn advancePinBoundary(self: Store, template: []const u8, root: []const u8, id: []const u8, state: *PinProjection, lookup: AliasLookup, static_only: bool, incremental: bool, remaining: *u32, scratch: *std.heap.FixedBufferAllocator) !bool {
        if (remaining.* < 12) return false;
        if (static_only) state.static_base = true;
        scratch.reset();
        const temporary = Store{ .a = scratch.allocator(), .backend = self.backend, .prefix = self.prefix };
        const manifest = try std.fmt.allocPrint(self.a, "{s}boundary/{s}", .{ root, try hash(self.a, id) });
        if (state.walk == null) {
            const node = if (try temporary.pinCount(template, id) != 0) try lookup.node.?(lookup.ptr, temporary.a, id) else null;
            if (node == null) {
                var cost: u32 = 1;
                if (incremental) if (try self.backend.get(self.backend.ptr, self.a, manifest)) |raw| {
                    const old = try parse(PinMember, self.a, raw);
                    try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}member/{s}/{s}", .{ root, try hash(self.a, old.root), try hash(self.a, id) }));
                    try self.backend.remove(self.backend.ptr, self.a, manifest);
                    cost = 2;
                };
                remaining.* -= cost;
                return true;
            }
            state.walk = .{ .key = try self.a.dupe(u8, id), .current = try self.a.dupe(u8, id), .root = if (static_only) try self.a.dupe(u8, node.?.canonical) else null };
        }
        const walk = &state.walk.?;
        if (!static_only) {
            try self.tracePinNode(root, walk.current);
            remaining.* -= 1;
        }
        if (!std.mem.eql(u8, walk.key, id)) return error.InvalidCatalogRecord;
        const path_key = try std.fmt.allocPrint(temporary.a, "{s}path/{s}", .{ root, try hash(temporary.a, walk.current) });
        const known = if (try self.backend.get(self.backend.ptr, temporary.a, path_key)) |value| try parse(PinPath, temporary.a, value) else null;
        if (known) |value| if (!std.mem.eql(u8, value.key, walk.current)) return error.InvalidCatalogRecord;
        if (walk.root) |terminal| {
            if (static_only or std.mem.eql(u8, walk.current, terminal) or known != null) {
                if (known) |value| if (!std.mem.eql(u8, value.root, terminal)) return error.InvalidCatalogRecord;
                if (incremental) {
                    if (try self.backend.get(self.backend.ptr, self.a, manifest)) |raw| {
                        const old = try parse(PinMember, self.a, raw);
                        try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}member/{s}/{s}", .{ root, try hash(self.a, old.root), try hash(self.a, id) }));
                    }
                    try self.repairItem(root, "root", terminal);
                }
                remaining.* -= if (incremental) @as(u32, 4) else 2;
                try self.putPinMember(root, .{ .root = terminal, .key = walk.key });
                state.walk = null;
                state.trace_clean = !static_only;
                return true;
            }
            const next = (try temporary.queryPinNext(template, lookup, walk.current)) orelse return error.InvalidCatalogRecord;
            remaining.* -= 7;
            try self.putPinPath(root, .{ .key = walk.current, .root = terminal, .next = next });
            walk.current = try self.a.dupe(u8, next);
        } else if (known) |value| {
            remaining.* -= 1;
            walk.root = try self.a.dupe(u8, value.root);
            walk.current = walk.key;
        } else if (try temporary.queryPinNext(template, lookup, walk.current)) |next| {
            remaining.* -= 1;
            if (walk.hops >= 16383) return error.ResolverCurationLimitExceeded;
            walk.hops += 1;
            walk.current = try self.a.dupe(u8, next);
        } else {
            remaining.* -= 6;
            // Roots themselves are dependencies of every path ending here.
            try self.putPinPath(root, .{ .key = walk.current, .root = walk.current });
            walk.root = walk.current;
            walk.current = walk.key;
        }
        return false;
    }
    /// The reverse dependency graph contains each configured/native path node
    /// once, rather than a copy of every complete boundary-to-root path.
    fn seedPinNode(self: Store, root: []const u8, id: []const u8) !void {
        const dirty = try std.fmt.allocPrint(self.a, "{s}dirty-node/{s}", .{ root, try hash(self.a, id) });
        if (try self.backend.get(self.backend.ptr, self.a, dirty) != null) return;
        const path = try std.fmt.allocPrint(self.a, "{s}path/{s}", .{ root, try hash(self.a, id) });
        const old_root = if (try self.backend.get(self.backend.ptr, self.a, path)) |raw| (try parse(PinPath, self.a, raw)).root else null;
        try self.backend.put(self.backend.ptr, self.a, dirty, try encode(self.a, DirtyPinNode{ .key = id, .root = old_root }));
        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}pending-node/{s}", .{ root, try hash(self.a, id) }), try encode(self.a, id));
        if (old_root) |value| try self.repairItem(root, "root", value);
    }
    fn advancePinRoute(self: Store, template: []const u8, root: []const u8, id: []const u8, state: *PinProjection, lookup: AliasLookup, incremental: bool, budget: *u32, scratch: *std.heap.FixedBufferAllocator) !bool {
        if (budget.* < 12) return false;
        scratch.reset();
        const temporary = Store{ .a = scratch.allocator(), .backend = self.backend, .prefix = self.prefix };
        if (state.walk == null) {
            if (try temporary.backend.get(temporary.backend.ptr, temporary.a, try std.fmt.allocPrint(temporary.a, "{s}path/{s}", .{ root, try hash(temporary.a, id) })) != null) {
                budget.* -= 1;
                return true;
            }
            if (state.static_only) {
                const node = try lookup.node.?(lookup.ptr, temporary.a, id);
                const terminal = if (node) |value| value.canonical else id;
                const next = try temporary.queryPinNext(template, lookup, id);
                try self.putPinPath(root, .{ .key = id, .root = terminal, .next = next });
                if (incremental) try self.repairItem(root, "root", terminal);
                budget.* -= 8;
                return true;
            }
            state.walk = .{ .key = try self.a.dupe(u8, id), .current = try self.a.dupe(u8, id) };
        }
        const walk = &state.walk.?;
        try self.tracePinNode(root, walk.current);
        budget.* -= 1;
        if (!std.mem.eql(u8, walk.key, id)) return error.InvalidCatalogRecord;
        const path = try std.fmt.allocPrint(temporary.a, "{s}path/{s}", .{ root, try hash(temporary.a, walk.current) });
        const known = if (try temporary.backend.get(temporary.backend.ptr, temporary.a, path)) |raw| try parse(PinPath, temporary.a, raw) else null;
        if (walk.root) |terminal| {
            if (known != null or std.mem.eql(u8, walk.current, terminal)) {
                if (known) |value| if (!std.mem.eql(u8, value.root, terminal)) return error.InvalidCatalogRecord;
                state.walk = null;
                state.trace_clean = true;
                budget.* -= 1;
                return true;
            }
            const next = (try temporary.queryPinNext(template, lookup, walk.current)) orelse return error.InvalidCatalogRecord;
            try self.putPinPath(root, .{ .key = walk.current, .root = terminal, .next = next });
            if (incremental) try self.repairItem(root, "root", terminal);
            budget.* -= 8;
            walk.current = try self.a.dupe(u8, next);
        } else if (known) |value| {
            walk.root = try self.a.dupe(u8, value.root);
            walk.current = walk.key;
            budget.* -= 1;
        } else if (try temporary.queryPinNext(template, lookup, walk.current)) |next| {
            if (walk.hops >= 16383) return error.ResolverCurationLimitExceeded;
            walk.hops += 1;
            walk.current = try self.a.dupe(u8, next);
            budget.* -= 1;
        } else {
            try self.putPinPath(root, .{ .key = walk.current, .root = walk.current });
            if (incremental) try self.repairItem(root, "root", walk.current);
            budget.* -= 8;
            walk.root = walk.current;
            walk.current = walk.key;
        }
        return false;
    }
    fn pinGuard(self: Store, template: []const u8, root: []const u8, id: []const u8, lookup: AliasLookup, incremental: bool) !u32 {
        const key = try std.fmt.allocPrint(self.a, "{s}guard/{s}", .{ root, try hash(self.a, id) });
        const prior = if (try self.backend.get(self.backend.ptr, self.a, key)) |raw| try parse(PinGuard, self.a, raw) else null;
        const Allow = struct {
            fn enabled(_: *anyopaque, _: A, _: []const u8) !bool {
                return true;
            }
        };
        var unused: u8 = 0;
        const edges = try lookup.expand(lookup.ptr, self.a, id, true, .{ .ptr = &unused, .enabled = Allow.enabled });
        if (edges.len > 1) return error.InvalidResolverAlias;
        if (edges.len == 0) {
            if (prior != null) return error.InvalidCatalogRecord;
            return 1;
        }
        const edge = edges[0];
        const parent = (try self.backend.get(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}path/{s}", .{ root, try hash(self.a, edge.target) }))) orelse return error.InvalidCatalogRecord;
        const own = (try self.backend.get(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}path/{s}", .{ root, try hash(self.a, id) }))) orelse return error.InvalidCatalogRecord;
        const terminal = (try parse(PinPath, self.a, parent)).root;
        const next: PinGuard = .{ .key = id, .parent = edge.target, .root = terminal, .latent = !std.mem.eql(u8, terminal, (try parse(PinPath, self.a, own)).root) };
        if (prior) |old| {
            if (!std.mem.eql(u8, old.parent, next.parent)) return error.InvalidCatalogRecord;
            if (old.latent == next.latent and std.mem.eql(u8, old.root, next.root)) return 1;
            if (old.latent) {
                try self.pinFilterUpdate(root, old.root, id, false);
                try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}root-guard/{s}/{s}", .{ root, try hash(self.a, old.root), try hash(self.a, id) }));
                if (incremental) try self.repairItem(root, "root", old.root);
            }
        } else {
            try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}guard-in/{s}/{s}", .{ root, try hash(self.a, next.parent), try hash(self.a, id) }), try encode(self.a, GuardEdge{ .key = id, .parent = next.parent }));
        }
        if (next.latent) {
            try self.pinFilterUpdate(root, next.root, id, true);
            try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}root-guard/{s}/{s}", .{ root, try hash(self.a, next.root), try hash(self.a, id) }), try encode(self.a, next));
            if (incremental) try self.repairItem(root, "root", next.root);
        }
        try self.backend.put(self.backend.ptr, self.a, key, try encode(self.a, next));
        _ = template;
        return if (!next.latent and (prior == null or !prior.?.latent)) (if (prior == null) @as(u32, 2) else 1) else 16;
    }
    /// Ingestion has its own durable watermark and bounded turn budget.
    /// New edits enter the next work set, never rewind an active work cursor.
    fn ingestPinEdits(self: Store, template: []const u8, root: []const u8, state: *PinProjection, latest: PinEditHead, lookup: AliasLookup, budget: *u32, scratch: *std.heap.FixedBufferAllocator) !void {
        const repair = &state.repair.?;
        // Event strings must survive every item, but lookup/point reads must
        // not accumulate across items (canonical keys may be 16 KiB escaped).
        scratch.reset();
        var event_frame: std.heap.FixedBufferAllocator = .init(scratch.buffer[0 .. 1024 * 1024]);
        var item_frame: std.heap.FixedBufferAllocator = .init(scratch.buffer[1024 * 1024 ..]);
        while (budget.* >= 2 and repair.sequence <= latest.sequence) {
            event_frame.reset();
            var event_reader = self;
            event_reader.a = event_frame.allocator();
            const event = (try event_reader.pinEdit(template, repair.sequence)) orelse return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, &event.before, &(repair.ingest_graph orelse state.graph))) return error.InvalidCatalogRecord;
            while (budget.* >= 2 and repair.sequence == event.sequence) {
                item_frame.reset();
                var temporary = self;
                temporary.a = item_frame.allocator();
                if (repair.index < event.boundaries.len) {
                    const id = event.boundaries[repair.index];
                    const digest = try hash(temporary.a, id);
                    if (try lookup.node.?(lookup.ptr, temporary.a, id) != null or try self.backend.get(self.backend.ptr, temporary.a, try std.fmt.allocPrint(temporary.a, "{s}boundary/{s}", .{ root, digest })) != null) {
                        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}next-boundary/{s}", .{ root, digest }), try encode(self.a, id));
                    }
                    repair.index += 1;
                    budget.* -= 1;
                } else if (repair.index < event.boundaries.len + event.routing.len) {
                    const id = event.routing[repair.index - event.boundaries.len];
                    const digest = try hash(temporary.a, id);
                    const configured = try lookup.node.?(lookup.ptr, temporary.a, id) != null;
                    const traced = if (state.walk != null) try self.backend.get(self.backend.ptr, temporary.a, try std.fmt.allocPrint(temporary.a, "{s}trace/{s}", .{ root, digest })) != null else false;
                    if (traced) state.walk.?.invalid = true;
                    const relevant = configured or traced or try self.backend.get(self.backend.ptr, temporary.a, try std.fmt.allocPrint(temporary.a, "{s}path/{s}", .{ root, digest })) != null or try self.backend.get(self.backend.ptr, temporary.a, try std.fmt.allocPrint(temporary.a, "{s}dirty-node/{s}", .{ root, digest })) != null;
                    if (relevant) {
                        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}next-node/{s}", .{ root, digest }), try encode(self.a, id));
                        if (!configured) try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}prune-node/{s}", .{ root, digest }), try encode(self.a, id));
                    }
                    repair.index += 1;
                    budget.* -= 2;
                } else if (repair.index < event.boundaries.len + event.routing.len + event.targets.len) {
                    const id = event.targets[repair.index - event.boundaries.len - event.routing.len];
                    const digest = try hash(temporary.a, id);
                    if (try lookup.node.?(lookup.ptr, temporary.a, id) == null and (try self.backend.get(self.backend.ptr, temporary.a, try std.fmt.allocPrint(temporary.a, "{s}path/{s}", .{ root, digest })) != null or try self.backend.get(self.backend.ptr, temporary.a, try std.fmt.allocPrint(temporary.a, "{s}dirty-node/{s}", .{ root, digest })) != null or try self.backend.get(self.backend.ptr, temporary.a, try std.fmt.allocPrint(temporary.a, "{s}trace/{s}", .{ root, digest })) != null)) {
                        try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}prune-node/{s}", .{ root, digest }), try encode(self.a, id));
                    }
                    repair.index += 1;
                    budget.* -= 1;
                } else {
                    repair.target = event.sequence;
                    repair.ingest_graph = event.graph;
                    repair.sequence += 1;
                    repair.index = 0;
                    budget.* -= 1;
                }
            }
        }
    }
    fn hasNextPinWork(self: Store, root: []const u8) !bool {
        return (try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "next-" }), null, 1)).rows.len != 0;
    }
    fn advancePinRepair(self: Store, template: []const u8, root: []const u8, state: *PinProjection, latest: PinEditHead, lookup: AliasLookup, limit: u32) !void {
        const scratch = try query_context.RoutingScratch.init();
        defer scratch.deinit();
        var frame: std.heap.FixedBufferAllocator = .init(scratch.bytes);
        var budget = limit;
        const repair = &state.repair.?;
        if (repair.stage == .prune or repair.stage == .cleanup) {
            if (repair.target != latest.sequence) repair.stage = .seed;
        }
        // An unconsumed head makes active graph work unsafe. Give ingestion
        // the remaining shared budget, then spend every leftover unit on repair.
        // A fixed ingress quota can otherwise starve repair while capacity sits idle.
        try self.ingestPinEdits(template, root, state, latest, lookup, &budget, &frame);
        if (repair.sequence <= latest.sequence) return;
        if (state.walk) |walk| if (walk.invalid) {
            state.walk = null;
            state.trace_clean = true;
        };
        while (budget >= 4) {
            try self.cleanPinTrace(root, state, &budget);
            if (state.trace_clean or budget < 4) return;
            switch (repair.stage) {
                .requeue => repair.stage = .seed, // Decode old cursors only.
                .seed => {
                    if (budget < 4) return;
                    var page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "next-boundary/" }), null, 1);
                    const boundary = page.rows.len != 0;
                    if (!boundary) page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "next-node/" }), null, 1);
                    if (page.rows.len == 0) {
                        repair.stage = .expand;
                        repair.after = null;
                        continue;
                    }
                    const id = try parse([]const u8, self.a, page.rows[0].value);
                    if (boundary) try self.seedPinBoundary(root, id) else try self.seedPinNode(root, id);
                    try self.backend.remove(self.backend.ptr, self.a, page.rows[0].key);
                    budget -= 4;
                },
                .expand => {
                    const pending = try std.mem.concat(self.a, u8, &.{ root, "pending-node/" });
                    if (repair.root == null) {
                        const page = try self.backend.scan(self.backend.ptr, self.a, pending, null, 1);
                        if (page.rows.len == 0) {
                            repair.stage = .paths;
                            repair.after = null;
                            continue;
                        }
                        repair.root = try parse([]const u8, self.a, page.rows[0].value);
                        repair.cursor = null;
                        try self.seedPinBoundary(root, repair.root.?);
                        budget -= 2;
                        if (budget < 4) return;
                    }
                    const incoming = try std.fmt.allocPrint(self.a, "{s}path-in/{s}/", .{ root, try hash(self.a, repair.root.?) });
                    const page = try self.backend.scan(self.backend.ptr, self.a, incoming, repair.cursor, @max(1, (budget - 1) / 3));
                    for (page.rows) |row| {
                        try self.seedPinNode(root, (try parse(PinPath, self.a, row.value)).key);
                        budget -= 3;
                    }
                    repair.cursor = page.next;
                    if (page.next == null) {
                        try self.backend.remove(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ pending, try hash(self.a, repair.root.?) }));
                        repair.root = null;
                        budget -= 1;
                    }
                },
                .paths => {
                    if (budget < 8) return;
                    const prefix = try std.mem.concat(self.a, u8, &.{ root, "dirty-node/" });
                    const page = try self.backend.scan(self.backend.ptr, self.a, prefix, repair.after, @max(1, budget / 8));
                    for (page.rows) |row| {
                        const id = (try parse(DirtyPinNode, self.a, row.value)).key;
                        const key = try std.fmt.allocPrint(self.a, "{s}path/{s}", .{ root, try hash(self.a, id) });
                        if (try self.backend.get(self.backend.ptr, self.a, key)) |raw| {
                            const path = try parse(PinPath, self.a, raw);
                            try self.pinFilterUpdate(root, path.root, path.key, false);
                            try self.backend.remove(self.backend.ptr, self.a, key);
                            try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}root-path/{s}/{s}", .{ root, try hash(self.a, path.root), try hash(self.a, path.key) }));
                            if (path.next) |next| try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}path-in/{s}/{s}", .{ root, try hash(self.a, next), try hash(self.a, path.key) }));
                        }
                        budget -= 8;
                        repair.after = row.key;
                    }
                    if (page.next == null) {
                        repair.stage = .nodes;
                        repair.after = null;
                    }
                    if (page.rows.len == 0) budget -= 1;
                },
                .nodes, .members => {
                    if (budget < 12) return;
                    const prefix = try std.mem.concat(self.a, u8, &.{ root, if (repair.stage == .nodes) "dirty-node/" else "dirty-boundary/" });
                    const page = try self.backend.scan(self.backend.ptr, self.a, prefix, repair.after, @max(1, budget / 12));
                    var offset: usize = 0;
                    while (offset < page.rows.len and budget >= 12) {
                        const row = page.rows[offset];
                        const id = if (repair.stage == .nodes) (try parse(DirtyPinNode, self.a, row.value)).key else try parse([]const u8, self.a, row.value);
                        const done = if (repair.stage == .nodes) try self.advancePinRoute(template, root, id, state, lookup, true, &budget, &frame) else try self.advancePinBoundary(template, root, id, state, lookup, state.static_only, true, &budget, &frame);
                        if (done) {
                            repair.after = row.key;
                            offset += 1;
                        }
                        if (state.trace_clean) break;
                    }
                    if (offset == page.rows.len and page.next == null) {
                        repair.stage = if (repair.stage == .nodes) .guard_expand else .retire;
                        repair.after = null;
                    }
                },
                .guard_expand => {
                    if (budget < 4) return;
                    if (repair.root == null) {
                        const page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "dirty-node/" }), repair.after, 1);
                        if (page.rows.len == 0) {
                            repair.stage = .guards;
                            repair.after = null;
                            continue;
                        }
                        repair.root = (try parse(DirtyPinNode, self.a, page.rows[0].value)).key;
                        repair.after = page.rows[0].key;
                        repair.cursor = null;
                        try self.repairItem(root, "guard", repair.root.?);
                        budget -= 1;
                    }
                    const incoming = try std.fmt.allocPrint(self.a, "{s}guard-in/{s}/", .{ root, try hash(self.a, repair.root.?) });
                    const page = try self.backend.scan(self.backend.ptr, self.a, incoming, repair.cursor, @max(1, budget));
                    for (page.rows) |row| {
                        try self.repairItem(root, "guard", (try parse(GuardEdge, self.a, row.value)).key);
                        budget -= 1;
                    }
                    repair.cursor = page.next;
                    if (page.next == null) repair.root = null;
                    if (page.rows.len == 0) budget -= 1;
                },
                .guards => {
                    if (budget < 16) return;
                    const page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "dirty-guard/" }), repair.after, 1);
                    if (page.rows.len == 0) {
                        repair.stage = .members;
                        repair.after = null;
                        continue;
                    }
                    budget -= try self.pinGuard(template, root, try parse([]const u8, self.a, page.rows[0].value), lookup, true);
                    repair.after = page.rows[0].key;
                },
                .retire => {
                    var page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "dirty-node/" }), null, budget);
                    if (page.rows.len == 0) page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "dirty-boundary/" }), null, budget);
                    if (page.rows.len == 0) page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "dirty-guard/" }), null, budget);
                    for (page.rows) |row| try self.backend.remove(self.backend.ptr, self.a, row.key);
                    budget -= @intCast(page.rows.len);
                    if (page.rows.len == 0) {
                        repair.stage = if (try self.hasNextPinWork(root)) .seed else .prune;
                        repair.after = null;
                    }
                },
                .prune => {
                    if (try self.hasNextPinWork(root)) {
                        repair.stage = .seed;
                        continue;
                    }
                    if (budget < 12) return;
                    const page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "prune-node/" }), null, 1);
                    if (page.rows.len == 0) {
                        // All ingested mutations are represented. Advance the
                        // exact baseline before bounded marker cleanup, so a
                        // subsequent head never replays completed components.
                        state.graph = latest.graph;
                        state.sequence = latest.sequence;
                        repair.stage = .cleanup;
                        continue;
                    }
                    const id = try parse([]const u8, self.a, page.rows[0].value);
                    const path = try std.fmt.allocPrint(self.a, "{s}path/{s}", .{ root, try hash(self.a, id) });
                    if (try self.backend.get(self.backend.ptr, self.a, path)) |raw| {
                        const node = try parse(PinPath, self.a, raw);
                        const declared = try lookup.node.?(lookup.ptr, self.a, id) != null;
                        if (!declared and (try self.backend.scan(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}path-in/{s}/", .{ root, try hash(self.a, id) }), null, 1)).rows.len == 0) {
                            try self.pinFilterUpdate(root, node.root, id, false);
                            if (node.next) |next| {
                                try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}path-in/{s}/{s}", .{ root, try hash(self.a, next), try hash(self.a, id) }));
                                try self.backend.put(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}prune-node/{s}", .{ root, try hash(self.a, next) }), try encode(self.a, next));
                            }
                            try self.backend.remove(self.backend.ptr, self.a, path);
                            try self.backend.remove(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}root-path/{s}/{s}", .{ root, try hash(self.a, node.root), try hash(self.a, id) }));
                        }
                    }
                    try self.backend.remove(self.backend.ptr, self.a, page.rows[0].key);
                    budget -= 12;
                },
                .cleanup => {
                    if (try self.hasNextPinWork(root)) {
                        repair.stage = .seed;
                        continue;
                    }
                    const page = try self.backend.scan(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "dirty-root/" }), null, budget);
                    for (page.rows) |row| try self.backend.remove(self.backend.ptr, self.a, row.key);
                    budget -= @intCast(page.rows.len);
                    if (page.rows.len == 0) {
                        state.graph = latest.graph;
                        state.sequence = latest.sequence;
                        state.phase = .ready;
                        state.repair = null;
                        return;
                    }
                },
            }
        }
    }
    fn buildPinNodes(self: Store, template: []const u8, root: []const u8, state: *PinProjection, lookup: AliasLookup, budget: *u32, frame: *std.heap.FixedBufferAllocator) !void {
        // Every immutable configured node is enrolled, including masked parents.
        // Native tails are discovered by the same resumable forward walks; an
        // unrelated native namespace never becomes a per-profile path copy.
        while (budget.* >= 12) {
            try self.cleanPinTrace(root, state, budget);
            if (state.trace_clean or budget.* < 12) return;
            var position: [64]u8 = @splat('0');
            if (state.after) |after| {
                @memcpy(&position, after);
                position[63] += 1;
            }
            const node = try lookup.seek_node.?(lookup.ptr, self.a, &position, null);
            if (node == null) {
                state.after = null;
                state.phase = .guards;
                budget.* -= 1;
                return;
            }
            if (try self.advancePinRoute(template, root, node.?.key, state, lookup, false, budget, frame)) state.after = try hash(self.a, node.?.key);
        }
    }
    fn newPinRepair(state: PinProjection) PinRepair {
        return .{ .sequence = state.sequence.? + 1, .target = state.sequence.?, .ingest_graph = state.graph };
    }
    fn buildPinGuards(self: Store, template: []const u8, root: []const u8, state: *PinProjection, lookup: AliasLookup, budget: *u32) !void {
        while (budget.* >= 16) {
            var position: [64]u8 = @splat('0');
            if (state.after) |after| {
                @memcpy(&position, after);
                position[63] += 1;
            }
            const node = try lookup.seek_node.?(lookup.ptr, self.a, &position, null);
            if (node == null) {
                state.after = null;
                state.phase = .build;
                budget.* -= 1;
                return;
            }
            budget.* -= try self.pinGuard(template, root, node.?.key, lookup, false);
            state.after = try hash(self.a, node.?.key);
        }
    }
    pub fn advanceQueryPins(self: Store, template: []const u8, profile: []const u8, lookup: AliasLookup, limit: u32) !bool {
        if (limit < 32 or limit > 4096 or lookup.node == null or lookup.seek_node == null) return error.InvalidResolverCurationRequest;
        const h = (try self.head(template)) orelse return true;
        const graph = (try self.queryGraph(h)).fingerprint;
        const journal = try std.mem.concat(self.a, u8, &.{ try self.pinEditRoot(template), "head" });
        const pins = if (try self.backend.get(self.backend.ptr, self.a, try self.pinReadyPath(template))) |raw| (try parse(PinReady, self.a, raw)).endpoints else 1;
        if (pins != 0) {
            const raw_head = try self.backend.get(self.backend.ptr, self.a, journal);
            var enrolled: PinEditHead = if (raw_head) |value| try parse(PinEditHead, self.a, value) else .{ .graph = graph };
            if (!enrolled.indexed or !enrolled.compact or !enrolled.tails_indexed or !enrolled.routing_indexed or enrolled.routing_origin == null) {
                enrolled.indexed = true;
                enrolled.compact = true;
                enrolled.tails_indexed = true;
                enrolled.routing_indexed = true;
                enrolled.routing_origin = enrolled.routing_origin orelse enrolled.graph;
                enrolled.barrier = enrolled.sequence;
                try self.backend.put(self.backend.ptr, self.a, journal, try encode(self.a, enrolled));
                return false;
            }
        }
        var pruned = false;
        if (!try self.pruneQueryPins(template, lookup, limit, &pruned)) return false;
        if (pins == 0) return true;
        const latest = try self.pinEditHead(template, graph);
        const root = try self.pinProjectionRoot(template, profile);
        const state_path = try std.mem.concat(self.a, u8, &.{ root, "state" });
        const raw = try self.backend.get(self.backend.ptr, self.a, state_path);
        var state: PinProjection = if (raw) |value| try parse(PinProjection, self.a, value) else .{ .graph = graph };
        if (state.format != pin_projection_format) state = .{ .graph = graph, .format = pin_projection_format, .sequence = latest.sequence };
        if (state.phase == .clear) {
            // No new graph data has been constructed yet. Moving the starting
            // cut cannot restart an in-progress clear or lose queued work.
            state.graph = graph;
            state.sequence = latest.sequence;
            state.repair = null;
            state.static_only = !try self.hasQueryOverrides(template);
        } else if (state.repair) |repair| {
            var cursor = state;
            cursor.sequence = repair.target;
            cursor.graph = repair.ingest_graph orelse state.graph;
            if (!try self.pinEditsAvailable(template, cursor, latest)) state = .{ .graph = graph, .format = pin_projection_format, .sequence = latest.sequence };
        } else if (!std.mem.eql(u8, &state.graph, &graph)) {
            if (try self.pinEditsAvailable(template, state, latest)) {
                state.repair = newPinRepair(state);
                if (state.phase == .ready) state.phase = .repair;
            } else state = .{ .graph = graph, .format = pin_projection_format, .sequence = latest.sequence };
        }
        if (state.phase == .ready) return true;
        if (pruned) return false;
        state.static_only = !try self.hasQueryOverrides(template);
        if (state.phase == .repair) try self.advancePinRepair(template, root, &state, latest, lookup, limit) else {
            if (state.phase == .clear) {
                const page = try self.backend.scan(self.backend.ptr, self.a, root, null, limit);
                var deleted: usize = 0;
                for (page.rows) |row| {
                    if (std.mem.eql(u8, row.key, state_path)) continue;
                    try self.backend.remove(self.backend.ptr, self.a, row.key);
                    deleted += 1;
                }
                if (page.next == null) state.phase = .nodes;
                if (deleted != 0) {
                    try self.backend.put(self.backend.ptr, self.a, state_path, try encode(self.a, state));
                    return false;
                }
            }
            const scratch = try query_context.RoutingScratch.init();
            defer scratch.deinit();
            var frame: std.heap.FixedBufferAllocator = .init(scratch.bytes);
            var budget = limit;
            if (state.repair != null) {
                try self.ingestPinEdits(template, root, &state, latest, lookup, &budget, &frame);
                if (state.repair.?.sequence <= latest.sequence) {
                    try self.backend.put(self.backend.ptr, self.a, state_path, try encode(self.a, state));
                    return false;
                }
                if (state.walk) |walk| if (walk.invalid) {
                    state.walk = null;
                    state.trace_clean = true;
                };
            }
            try self.cleanPinTrace(root, &state, &budget);
            if (!state.trace_clean) {
                if (state.phase == .nodes) try self.buildPinNodes(template, root, &state, lookup, &budget, &frame);
                if (state.phase == .guards) try self.buildPinGuards(template, root, &state, lookup, &budget);
                if (state.phase == .build) {
                    // Merge-seek the immutable node and native pin indexes:
                    // either side can skip an arbitrarily large unrelated gap.
                    // The exclusive digest cursor also resumes older posting
                    // passes, without changing the persisted format.
                    const pin_prefix = try self.keyPath(try std.fmt.allocPrint(self.a, "pin-count/{s}/", .{try hash(self.a, template)}));
                    while (budget >= 12) {
                        frame.reset();
                        var digest: [64]u8 = undefined;
                        const key = if (state.walk) |walk| blk: {
                            // A count may disappear during a paused boundary
                            // walk. Finish it before queued repair removes the
                            // obsolete member; never switch the walk's key.
                            const retained = try self.a.dupe(u8, walk.key);
                            @memcpy(&digest, try hash(frame.allocator(), retained));
                            break :blk retained;
                        } else blk: {
                            const temp = frame.allocator();
                            var position: [64]u8 = @splat('0');
                            if (state.after) |after| {
                                @memcpy(&position, after);
                                position[63] += 1;
                            }
                            var node = (try lookup.seek_node.?(lookup.ptr, temp, &position, null)) orelse {
                                state.after = null;
                                state.phase = if (state.repair != null) .repair else .ready;
                                budget -= 1;
                                break;
                            };
                            var node_digest: [64]u8 = undefined;
                            @memcpy(&node_digest, try hash(temp, node.key));
                            const before = try std.mem.concat(temp, u8, &.{ pin_prefix, &node_digest });
                            before[before.len - 1] -= 1;
                            const page = try self.backend.scan(self.backend.ptr, temp, pin_prefix, before, 1);
                            if (page.rows.len == 0) {
                                state.after = null;
                                state.phase = if (state.repair != null) .repair else .ready;
                                budget -= 1;
                                break;
                            }
                            const posting = page.rows[0];
                            if (!std.mem.startsWith(u8, posting.key, pin_prefix) or posting.key.len != pin_prefix.len + 64) return error.InvalidCatalogRecord;
                            const pin_digest = posting.key[pin_prefix.len..];
                            if (!std.mem.eql(u8, pin_digest, &node_digest)) {
                                node = (try lookup.seek_node.?(lookup.ptr, temp, pin_digest, null)) orelse {
                                    state.after = null;
                                    state.phase = if (state.repair != null) .repair else .ready;
                                    budget -= 1;
                                    break;
                                };
                                @memcpy(&node_digest, try hash(temp, node.key));
                                if (!std.mem.eql(u8, pin_digest, &node_digest)) {
                                    // This native posting is provably outside
                                    // the profile. Persist the skipped gap.
                                    state.after = try self.a.dupe(u8, pin_digest);
                                    budget -= 1;
                                    continue;
                                }
                            }
                            const count = try parse(PinCount, temp, posting.value);
                            if (count.count == 0 or !std.mem.eql(u8, count.key, node.key)) return error.InvalidCatalogRecord;
                            digest = node_digest;
                            // The boundary walker resets frame. Retain only
                            // its key, never a decoded canonical node.
                            break :blk try self.a.dupe(u8, node.key);
                        };
                        if (try self.advancePinBoundary(template, root, key, &state, lookup, state.static_only, false, &budget, &frame)) state.after = try self.a.dupe(u8, &digest);
                        if (state.trace_clean) {
                            try self.cleanPinTrace(root, &state, &budget);
                            if (state.trace_clean) break;
                        }
                    }
                }
            }
        }
        try self.backend.put(self.backend.ptr, self.a, state_path, try encode(self.a, state));
        return state.phase == .ready;
    }
    fn queryPinNext(self: Store, template: []const u8, lookup: AliasLookup, id: []const u8) !?[]const u8 {
        if (try self.get(resolver.KeyAlias, template, .redirect, id)) |edge| return edge.target;
        if (try self.get([]const u8, template, .removed_alias, id) != null) return null;
        const Allow = struct {
            fn enabled(_: *anyopaque, _: A, _: []const u8) !bool {
                return true;
            }
        };
        var unused: u8 = 0;
        const edges = try lookup.expand(lookup.ptr, self.a, id, true, .{ .ptr = &unused, .enabled = Allow.enabled });
        if (edges.len == 0) return null;
        if (edges.len != 1 or !std.mem.eql(u8, edges[0].key, id)) return error.InvalidResolverAlias;
        return edges[0].target;
    }
    fn uniquePinDigests(bytes: []u8, frame: *std.heap.FixedBufferAllocator) !usize {
        const digests = std.mem.bytesAsSlice([32]u8, bytes);
        if (digests.len == 0) return 0;
        frame.reset();
        const slots = try std.math.ceilPowerOfTwo(usize, @max(8, digests.len + digests.len / 2));
        const table = try frame.allocator().alloc(u32, slots);
        @memset(table, 0);
        var unique: usize = 0;
        for (digests) |digest| {
            var slot: usize = @intCast(std.mem.readInt(u64, digest[0..8], .little) & (slots - 1));
            while (table[slot] != 0 and !std.mem.eql(u8, &digests[table[slot] - 1], &digest)) slot = (slot + 1) & (slots - 1);
            if (table[slot] != 0) continue;
            digests[unique] = digest;
            table[slot] = @intCast(unique + 1);
            unique += 1;
        }
        std.mem.sortUnstable([32]u8, digests[0..unique], {}, struct {
            fn less(_: void, l: [32]u8, r: [32]u8) bool {
                return std.mem.lessThan(u8, &l, &r);
            }
        }.less);
        return unique * 32;
    }
    fn pinRootTouched(self: Store, root: []const u8, terminal: []const u8, key: []const u8, digest: []const u8, template: []const u8) !bool {
        if (std.mem.eql(u8, terminal, key)) return true;
        const root_hash = try hash(self.a, terminal);
        if (try self.backend.get(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}root-path/{s}/{s}", .{ root, root_hash, digest })) != null) return true;
        if (try self.backend.get(self.backend.ptr, self.a, try std.fmt.allocPrint(self.a, "{s}root-guard/{s}/{s}", .{ root, root_hash, digest })) == null) return false;
        // Latent dependencies matter only when the configured edge is active.
        // All native reads use the caller's reusable item/point scratch.
        return try self.get(resolver.KeyAlias, template, .redirect, key) == null and try self.get([]const u8, template, .removed_alias, key) == null;
    }
    const PinTailMemo = struct {
        values: [][32]u8,
        slots: []u32,
        len: usize = 0,
        fn slot(self: *const PinTailMemo, digest: *const [32]u8) usize {
            var i: usize = @intCast(std.mem.readInt(u64, digest[0..8], .little) & (self.slots.len - 1));
            while (self.slots[i] != 0 and !std.mem.eql(u8, &self.values[self.slots[i] - 1], digest)) i = (i + 1) & (self.slots.len - 1);
            return i;
        }
        fn contains(self: *const PinTailMemo, digest: *const [32]u8) bool {
            return self.slots[self.slot(digest)] != 0;
        }
        fn remember(self: *PinTailMemo, digest: *const [32]u8) void {
            const i = self.slot(digest);
            if (self.slots[i] != 0 or self.len == self.values.len) return;
            self.values[self.len] = digest.*;
            self.slots[i] = @intCast(self.len + 1);
            self.len += 1;
        }
    };
    const PinTailLoader = struct {
        store: Store,
        prepared: PreparedQuery,
        lookup: AliasLookup,
        key: []const u8,
        memo: *PinTailMemo,
        visited: [][32]u8,
        visited_len: usize = 0,
        remaining: *u32,
        sequence: u64,
        io: *std.heap.FixedBufferAllocator,
        current: [16384]u8 = undefined,
        fn resolve(self: *@This()) ![]const u8 {
            var len = self.key.len;
            @memcpy(self.current[0..len], self.key);
            var temporary = self.store;
            temporary.a = self.io.allocator();
            while (true) {
                var digest: [32]u8 = undefined;
                @import("antfly_hash").Sha256.hash(self.current[0..len], &digest, .{});
                // This memo is request-local and only proves absence for its
                // requested roots. Never publish that partial result in cache.
                if (self.memo.contains(&digest)) return error.PinTailAlreadyClear;
                if (self.remaining.* == 0) return error.PinTailWorkExceeded;
                self.remaining.* -= 1;
                if (builtin.is_test) _ = test_tail_forward_steps.fetchAdd(1, .monotonic);
                self.visited[self.visited_len] = digest;
                self.visited_len += 1;
                self.io.reset();
                const next = (try temporary.queryPinNext(self.prepared.template, self.lookup, self.current[0..len])) orelse return self.current[0..len];
                try c.validKey(next);
                len = next.len;
                @memcpy(self.current[0..len], next);
            }
        }
        fn remember(self: *@This()) void {
            for (self.visited[0..self.visited_len]) |*digest| self.memo.remember(digest);
            var digest: [32]u8 = undefined;
            @import("antfly_hash").Sha256.hash(self.key, &digest, .{});
            self.memo.remember(&digest);
        }
        fn load(ptr: *anyopaque, a: A) !query_context.Graph {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const terminal = try self.resolve();
            const result_routes = try a.alloc(query_context.Route, 1);
            result_routes[0] = .{ .root = try a.dupe(u8, terminal), .component = try a.dupe(u8, self.key) };
            const dependencies = try a.dupe([32]u8, self.visited[0..self.visited_len]);
            std.mem.sortUnstable([32]u8, dependencies, {}, struct {
                fn less(_: void, l: [32]u8, r: [32]u8) bool {
                    return std.mem.lessThan(u8, &l, &r);
                }
            }.less);
            return .{ .routes = result_routes, .canonical = .{ .sequence = self.sequence, .graph = self.prepared.graph_id, .routing = self.prepared.routing_id, .dependencies = dependencies } };
        }
        fn cacheKey(prepared: PreparedQuery, origin: [32]u8, key: []const u8) ![32]u8 {
            var buffer: [1024]u8 = undefined;
            var writer: std.Io.Writer.Hashing(@import("antfly_hash").Sha256) = .init(&buffer);
            try std.json.Stringify.value(.{ "pin-source-canonical-v2", prepared.template, prepared.profile_id, origin, key }, .{}, &writer.writer);
            try writer.writer.flush();
            var result: [32]u8 = undefined;
            writer.hasher.final(&result);
            return result;
        }
    };
    const RoutingChanges = struct {
        const Change = struct { digest: [32]u8, sequence: u64 };
        entries: []Change = &.{},
        first: ?u64 = null,
        capacity: []Change = &.{},
        storage: []u8,
        fn load(self: *@This(), store: Store, template: []const u8, journal_head: PinEditHead, since: u64, frame: *std.heap.FixedBufferAllocator) !void {
            const previous = self.first orelse journal_head.sequence;
            if (since >= previous) return;
            if (since < journal_head.barrier or journal_head.sequence - since > 256) return error.InvalidCatalogRecord;
            if (self.first == null) {
                var workspace: std.heap.FixedBufferAllocator = .init(self.storage);
                self.capacity = try workspace.allocator().alloc(Change, 256 * 128);
            }
            const items = self.capacity;
            var count = self.entries.len;
            var sequence = since;
            // Extend only the missing prefix. Every required journal slot is
            // read at most once, even when cached paths have different ages.
            while (sequence < previous) {
                sequence += 1;
                frame.reset();
                var reader = store;
                reader.a = frame.allocator();
                const raw = (try reader.backend.get(reader.backend.ptr, reader.a, try std.fmt.allocPrint(reader.a, "{s}routed-compact/{d:0>3}", .{ try reader.pinEditRoot(template), sequence % 256 }))) orelse return error.InvalidCatalogRecord;
                if (raw.len < 8 or raw.len > 8 + 128 * 32 or (raw.len - 8) % 32 != 0 or std.mem.readInt(u64, raw[0..8], .little) != sequence) return error.InvalidCatalogRecord;
                var offset: usize = 8;
                while (offset < raw.len) : (offset += 32) {
                    items[count] = .{ .digest = raw[offset..][0..32].*, .sequence = sequence };
                    count += 1;
                }
            }
            std.mem.sortUnstable(Change, items[0..count], {}, struct {
                fn less(_: void, l: Change, r: Change) bool {
                    return std.mem.lessThan(u8, &l.digest, &r.digest);
                }
            }.less);
            var unique: usize = 0;
            for (items[0..count]) |item| {
                if (unique != 0 and std.mem.eql(u8, &items[unique - 1].digest, &item.digest)) {
                    items[unique - 1].sequence = @max(items[unique - 1].sequence, item.sequence);
                } else {
                    items[unique] = item;
                    unique += 1;
                }
            }
            self.entries = items[0..unique];
            self.first = since;
        }
        fn affects(self: @This(), dependencies: []const [32]u8, sequence: u64) bool {
            if (dependencies.len < self.entries.len) {
                for (dependencies) |digest| {
                    var lo: usize = 0;
                    var hi = self.entries.len;
                    while (lo < hi) {
                        const mid = lo + (hi - lo) / 2;
                        if (std.mem.lessThan(u8, &self.entries[mid].digest, &digest)) lo = mid + 1 else hi = mid;
                    }
                    if (lo < self.entries.len and self.entries[lo].sequence > sequence and std.mem.eql(u8, &self.entries[lo].digest, &digest)) return true;
                }
            } else for (self.entries) |item| {
                if (item.sequence <= sequence) continue;
                var lo: usize = 0;
                var hi = dependencies.len;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (std.mem.lessThan(u8, &dependencies[mid], &item.digest)) lo = mid + 1 else hi = mid;
                }
                if (lo < dependencies.len and std.mem.eql(u8, &dependencies[lo], &item.digest)) return true;
            }
            return false;
        }
    };
    const RoutingSources = struct {
        store: Store,
        sequence: u64,
        raw_prefix: []const u8,
        native_prefix: []const u8,
        coverage: [3][]const u8,
        raw: bool,
        done: bool = false,
        kind: usize = 0,
        digest: ?[64]u8 = null,
        cursor: [85]u8 = undefined,
        cursor_len: usize = 0,
        fn init(store: Store, prepared: PreparedQuery, sequence: u64, frame: *std.heap.FixedBufferAllocator) !@This() {
            const journal = try store.pinEditRoot(prepared.template);
            const prefix = try std.mem.concat(store.a, u8, &.{ journal, "routed/" });
            var result: @This() = .{ .store = store, .sequence = sequence, .raw_prefix = prefix, .native_prefix = try std.mem.concat(store.a, u8, &.{ journal, "routed-key/" }), .coverage = .{ "", "", "" }, .raw = true };
            result.cursor_len = (try std.fmt.bufPrint(&result.cursor, "{d:0>20}/~", .{sequence})).len;
            if (prepared.profile_id.len != 64) return result;
            frame.reset();
            const a = frame.allocator();
            const page = try store.backend.scan(store.backend.ptr, a, prefix, try std.mem.concat(a, u8, &.{ prefix, result.cursor[0..result.cursor_len] }), 9);
            if (page.next == null and page.rows.len <= 8) return result;
            result.raw = false;
            const root = try store.pinProjectionRoot(prepared.template, prepared.profile_id);
            inline for (.{ "path/", "dirty-node/", "trace/" }, 0..) |suffix, i| result.coverage[i] = try std.mem.concat(store.a, u8, &.{ root, suffix });
            return result;
        }
        fn relevant(self: @This(), prepared: PreparedQuery, digest: []const u8, frame: *std.heap.FixedBufferAllocator) !bool {
            if (prepared.profile_id.len != 64) return true;
            frame.reset();
            const a = frame.allocator();
            const root = try self.store.pinProjectionRoot(prepared.template, prepared.profile_id);
            inline for (.{ "path/", "dirty-node/", "trace/" }) |kind| if (try self.store.backend.get(self.store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}{s}{s}", .{ root, kind, digest })) != null) return true;
            return false;
        }
        fn next(self: *@This(), prepared: PreparedQuery, frame: *std.heap.FixedBufferAllocator, points: *std.heap.FixedBufferAllocator) !?PinInvalidation {
            while (!self.done) {
                frame.reset();
                const a = frame.allocator();
                if (self.raw) {
                    const after = try std.mem.concat(a, u8, &.{ self.raw_prefix, self.cursor[0..self.cursor_len] });
                    const page = try self.store.backend.scan(self.store.backend.ptr, a, self.raw_prefix, after, 1);
                    if (page.rows.len == 0) {
                        self.done = true;
                        return null;
                    }
                    const row = page.rows[0];
                    const suffix = row.key[self.raw_prefix.len..];
                    if (suffix.len != self.cursor.len) return error.InvalidCatalogRecord;
                    @memcpy(&self.cursor, suffix);
                    self.cursor_len = suffix.len;
                    const source = try parse(PinInvalidation, a, row.value);
                    if (source.sequence <= self.sequence) return error.InvalidCatalogRecord;
                    if (!try self.relevant(prepared, suffix[21..], points)) continue;
                    return source;
                }
                if (self.kind == self.coverage.len) {
                    self.done = true;
                    return null;
                }
                const prefix = self.coverage[self.kind];
                const after = if (self.digest) |digest| try std.mem.concat(a, u8, &.{ prefix, &digest }) else null;
                const page = try self.store.backend.scan(self.store.backend.ptr, a, prefix, after, 1);
                if (page.rows.len == 0) {
                    self.kind += 1;
                    self.digest = null;
                    continue;
                }
                var covered = page.rows[0];
                if (covered.key.len != prefix.len + 64) return error.InvalidCatalogRecord;
                const before = try std.mem.concat(a, u8, &.{ self.native_prefix, covered.key[prefix.len..] });
                before[before.len - 1] -= 1;
                const native_page = try self.store.backend.scan(self.store.backend.ptr, a, self.native_prefix, before, 1);
                if (native_page.rows.len == 0) {
                    self.kind += 1;
                    self.digest = null;
                    continue;
                }
                const native = native_page.rows[0];
                if (native.key.len != self.native_prefix.len + 64) return error.InvalidCatalogRecord;
                const native_digest = native.key[self.native_prefix.len..];
                if (!std.mem.eql(u8, native_digest, covered.key[prefix.len..])) {
                    const lower = try std.mem.concat(a, u8, &.{ prefix, native_digest });
                    lower[lower.len - 1] -= 1;
                    const next_page = try self.store.backend.scan(self.store.backend.ptr, a, prefix, lower, 1);
                    if (next_page.rows.len == 0) {
                        self.kind += 1;
                        self.digest = null;
                        continue;
                    }
                    covered = next_page.rows[0];
                    if (covered.key.len != prefix.len + 64) return error.InvalidCatalogRecord;
                    if (!std.mem.eql(u8, native_digest, covered.key[prefix.len..])) {
                        self.digest = native_digest[0..64].*;
                        continue;
                    }
                }
                self.digest = covered.key[prefix.len..][0..64].*;
                const sequence = try parse(u64, a, native.value);
                if (sequence <= self.sequence) continue;
                const key = switch (self.kind) {
                    0 => (try parse(PinPath, a, covered.value)).key,
                    1 => (try parse(DirtyPinNode, a, covered.value)).key,
                    2 => try parse([]const u8, a, covered.value),
                    else => unreachable,
                };
                if (!std.mem.eql(u8, native_digest, try hash(a, key))) return error.InvalidCatalogRecord;
                return .{ .sequence = sequence, .key = key };
            }
            return null;
        }
    };
    fn tailProofCurrent(self: Store, prepared: PreparedQuery, journal_head: PinEditHead, proof: query_context.CanonicalProof, changes: *RoutingChanges, events: *std.heap.FixedBufferAllocator) !bool {
        if (std.mem.eql(u8, &proof.routing, &prepared.routing_id)) return true;
        if (proof.sequence > journal_head.sequence or journal_head.sequence - proof.sequence > 256 or proof.sequence < journal_head.barrier) return false;
        if (proof.sequence == journal_head.sequence) return std.mem.eql(u8, &proof.graph, &journal_head.graph);
        events.reset();
        var reader = self;
        reader.a = events.allocator();
        const first = (try reader.pinEdit(prepared.template, proof.sequence + 1)) orelse return false;
        if (!std.mem.eql(u8, &first.before, &proof.graph)) return false;
        try changes.load(self, prepared.template, journal_head, proof.sequence, events);
        return !changes.affects(proof.dependencies, proof.sequence);
    }
    // Gate incoming discovery with chained targets, but resolve edited sources
    // that intersect the profile's published or in-flight dependency coverage.
    fn incomingPinRootsCurrent(self: Store, prepared: PreparedQuery, sequence: u64, latest_sequence: u64, roots: std.StringHashMapUnmanaged(void), lookup: AliasLookup, events: *std.heap.FixedBufferAllocator, points: *std.heap.FixedBufferAllocator, memo_frame: *std.heap.FixedBufferAllocator, storage: []u8) !bool {
        events.reset();
        var reader = self;
        reader.a = events.allocator();
        const journal_head = try reader.pinEditHead(prepared.template, prepared.graph_id);
        if (!journal_head.indexed or !journal_head.compact or !journal_head.routing_indexed or journal_head.routing_origin == null or !journal_head.tails_indexed or journal_head.sequence < sequence or journal_head.sequence - sequence > 256 or journal_head.barrier > sequence or journal_head.sequence != latest_sequence or !std.mem.eql(u8, &journal_head.graph, &prepared.graph_id)) return false;
        const trigger = try std.mem.concat(reader.a, u8, &.{ try reader.pinEditRoot(prepared.template), "tails/" });
        const gate = try self.backend.scan(self.backend.ptr, reader.a, trigger, try std.fmt.allocPrint(reader.a, "{s}{d:0>20}/~", .{ trigger, sequence }), 1);
        if (gate.rows.len == 0) return true;
        var sources = try RoutingSources.init(self, prepared, sequence, events);
        memo_frame.reset();
        const a = memo_frame.allocator();
        const slots = try a.alloc(u32, 32768);
        @memset(slots, 0);
        var memo: PinTailMemo = .{ .values = try a.alloc([32]u8, 16384), .slots = slots };
        const visited = std.mem.bytesAsSlice([32]u8, points.buffer[0 .. 512 * 1024]);
        var io_frame: std.heap.FixedBufferAllocator = .init(points.buffer[512 * 1024 ..]);
        var changes: RoutingChanges = .{ .storage = storage };
        var remaining: u32 = 16384;
        while (try sources.next(prepared, events, points)) |source| {
            if (source.sequence > journal_head.sequence) return error.InvalidCatalogRecord;
            try c.validKey(source.key);
            // Cache validation reuses event I/O. No source key may borrow it.
            var key_bytes: [16384]u8 = undefined;
            @memcpy(key_bytes[0..source.key.len], source.key);
            const key = key_bytes[0..source.key.len];
            var entry_digest: [32]u8 = undefined;
            @import("antfly_hash").Sha256.hash(key, &entry_digest, .{});
            if (memo.contains(&entry_digest)) continue;
            var loader: PinTailLoader = .{ .store = self, .prepared = prepared, .lookup = lookup, .key = key, .memo = &memo, .visited = visited, .remaining = &remaining, .io = &io_frame, .sequence = journal_head.sequence };
            const cache_key = try PinTailLoader.cacheKey(prepared, journal_head.routing_origin.?, key);
            var cached: ?query_context.Lease = null;
            defer if (cached) |lease| lease.release();
            var attempts: u8 = 0;
            if (prepared.profile_id.len == 64) {
                while (attempts < 2) : (attempts += 1) {
                    cached = query_context.tryTailCanonical(cache_key, .{ .ptr = &loader, .load = PinTailLoader.load }) catch |err| switch (err) {
                        error.PinTailAlreadyClear => {
                            loader.remember();
                            break;
                        },
                        error.PinTailWorkExceeded, error.OutOfMemory => return false,
                        else => return err,
                    };
                    const lease = cached orelse break;
                    const proof = lease.context.canonical;
                    if (proof != null and lease.context.routes.len == 1 and try self.tailProofCurrent(prepared, journal_head, proof.?, &changes, events)) break;
                    // Older snapshots do not displace a newer cached generation.
                    const replace = proof == null or proof.?.sequence <= journal_head.sequence;
                    if (replace) query_context.invalidateTailCanonical(cache_key, lease.context);
                    lease.release();
                    cached = null;
                    if (!replace) break;
                }
            }
            if (memo.contains(&entry_digest)) continue;
            if (cached) |lease| {
                if (roots.contains(lease.context.routes[0].root)) return false;
            } else {
                const terminal = loader.resolve() catch |err| switch (err) {
                    error.PinTailAlreadyClear => {
                        loader.remember();
                        continue;
                    },
                    error.PinTailWorkExceeded, error.OutOfMemory => return false,
                    else => return err,
                };
                if (roots.contains(terminal)) return false;
            }
            loader.remember();
        }
        return true;
    }
    fn pinRootsCurrent(self: Store, prepared: PreparedQuery, state: PinProjection, roots: std.StringHashMapUnmanaged(void), lookup: AliasLookup) !bool {
        if (state.format != pin_projection_format or (state.phase != .ready and state.phase != .repair)) return false;
        const sequence = state.sequence orelse return false;
        const scratch = try query_context.RoutingScratch.init();
        defer scratch.deinit();
        const change_capacity = 256 * 640 * 32;
        var root_frame: std.heap.FixedBufferAllocator = .init(scratch.bytes[change_capacity .. 6 * 1024 * 1024]);
        var event_frame: std.heap.FixedBufferAllocator = .init(scratch.bytes[6 * 1024 * 1024 .. 7 * 1024 * 1024]);
        var point_frame: std.heap.FixedBufferAllocator = .init(scratch.bytes[7 * 1024 * 1024 ..]);
        var temporary = self;
        temporary.a = event_frame.allocator();
        const latest = try temporary.pinEditHead(prepared.template, prepared.graph_id);
        if (!latest.indexed or !latest.compact or !latest.tails_indexed or !latest.routing_indexed or latest.routing_origin == null or latest.barrier > sequence or latest.sequence < sequence or latest.sequence - sequence > 256 or !std.mem.eql(u8, &latest.graph, &prepared.graph_id)) return false;
        if (sequence == latest.sequence) return std.mem.eql(u8, &state.graph, &latest.graph);
        const first = (try temporary.pinEdit(prepared.template, sequence + 1)) orelse return false;
        if (!std.mem.eql(u8, &first.before, &state.graph)) return false;
        const root = try self.pinProjectionRoot(prepared.template, prepared.profile_id);
        var selected = roots.keyIterator();
        while (selected.next()) |id| {
            point_frame.reset();
            const a = point_frame.allocator();
            if (try self.backend.get(self.backend.ptr, a, try std.fmt.allocPrint(a, "{s}dirty-root/{s}", .{ root, try hash(a, id.*) })) != null) return false;
        }
        event_frame.reset();
        const a = event_frame.allocator();
        const prefix = try std.mem.concat(a, u8, &.{ try temporary.pinEditRoot(prepared.template), "invalidated/" });
        const after = try std.fmt.allocPrint(a, "{s}{d:0>20}/~", .{ prefix, sequence });
        const page = try self.backend.scan(self.backend.ptr, a, prefix, after, 32);
        var changed_bytes: usize = 0;
        if (page.next == null) {
            // One/two edits stay linear in roots. Larger point proofs have a
            // fixed cross-product cap; batch root sets use dependency summaries.
            if (page.rows.len <= 2 or roots.count() <= 128 / page.rows.len) {
                for (page.rows) |row| {
                    root_frame.reset();
                    const edit = try parse(PinInvalidation, root_frame.allocator(), row.value);
                    if (edit.sequence <= sequence or edit.sequence > latest.sequence) return error.InvalidCatalogRecord;
                    const digest = try hash(root_frame.allocator(), edit.key);
                    var iterator = roots.keyIterator();
                    while (iterator.next()) |id| {
                        point_frame.reset();
                        temporary.a = point_frame.allocator();
                        if (try temporary.pinRootTouched(root, id.*, edit.key, digest, prepared.template)) return false;
                    }
                }
                return self.incomingPinRootsCurrent(prepared, sequence, latest.sequence, roots, lookup, &event_frame, &point_frame, &root_frame, scratch.bytes[0..change_capacity]);
            }
            // The complete deduplicated page already names every changed key.
            // Reuse it directly instead of reopening every retained ring event.
            for (page.rows) |row| {
                root_frame.reset();
                const edit = try parse(PinInvalidation, root_frame.allocator(), row.value);
                if (edit.sequence <= sequence or edit.sequence > latest.sequence) return error.InvalidCatalogRecord;
                @import("antfly_hash").Sha256.hash(edit.key, scratch.bytes[changed_bytes..][0..32], .{});
                changed_bytes += 32;
            }
        } else {
            // Large bursts carry only compact full SHA-256 fingerprints from
            // the bounded journal. Every summary hit still gets an exact read.
            var next = sequence;
            while (next < latest.sequence) {
                next += 1;
                event_frame.reset();
                const e = event_frame.allocator();
                temporary.a = e;
                const key = try std.fmt.allocPrint(e, "{s}compact/{d:0>3}", .{ try temporary.pinEditRoot(prepared.template), next % 256 });
                const raw = (try self.backend.get(self.backend.ptr, e, key)) orelse return false;
                if (raw.len < 8 or (raw.len - 8) % 32 != 0 or raw.len > 8 + 640 * 32 or std.mem.readInt(u64, raw[0..8], .little) != next or changed_bytes + raw.len - 8 > change_capacity) return error.InvalidCatalogRecord;
                @memcpy(scratch.bytes[changed_bytes..][0 .. raw.len - 8], raw[8..]);
                changed_bytes += raw.len - 8;
            }
        }
        changed_bytes = try uniquePinDigests(scratch.bytes[0..changed_bytes], &point_frame);
        var iterator = roots.keyIterator();
        while (iterator.next()) |id| {
            root_frame.reset();
            const r = root_frame.allocator();
            temporary.a = r;
            const root_hash = try hash(r, id.*);
            var own_digest: [32]u8 = undefined;
            @import("antfly_hash").Sha256.hash(id.*, &own_digest, .{});
            const own_hash = std.fmt.bytesToHex(own_digest, .lower);
            if (try self.backend.get(self.backend.ptr, r, try std.fmt.allocPrint(r, "{s}invalid-key/{s}", .{ try temporary.pinEditRoot(prepared.template), own_hash }))) |value| if (try parse(u64, r, value) > sequence) return false;
            var summary = (try self.pinFilter(root).reader(r, root_hash)) orelse continue;
            var matches = summary.intersection(std.mem.bytesAsSlice([32]u8, scratch.bytes[0..changed_bytes]));
            defer if (builtin.is_test) {
                _ = test_pin_summary_probes.fetchAdd(matches.probes, .monotonic);
            };
            while (true) {
                point_frame.reset();
                const p = point_frame.allocator();
                const digest = (try matches.next(&point_frame)) orelse break;
                temporary.a = p;
                const encoded = std.fmt.bytesToHex(digest.*, .lower);
                if (try self.backend.get(self.backend.ptr, p, try std.fmt.allocPrint(p, "{s}root-path/{s}/{s}", .{ root, root_hash, encoded })) != null) return false;
                if (try self.backend.get(self.backend.ptr, p, try std.fmt.allocPrint(p, "{s}root-guard/{s}/{s}", .{ root, root_hash, encoded })) == null) continue;
                const version = (try self.backend.get(self.backend.ptr, p, try std.fmt.allocPrint(p, "{s}invalid-key/{s}", .{ try temporary.pinEditRoot(prepared.template), encoded }))) orelse return error.InvalidCatalogRecord;
                const raw = (try self.backend.get(self.backend.ptr, p, try temporary.invalidationPath(prepared.template, try parse(u64, p, version), &encoded))) orelse return error.InvalidCatalogRecord;
                const edit = try parse(PinInvalidation, p, raw);
                if (edit.sequence <= sequence) return error.InvalidCatalogRecord;
                if (try temporary.get(resolver.KeyAlias, prepared.template, .redirect, edit.key) == null and try temporary.get([]const u8, prepared.template, .removed_alias, edit.key) == null) return false;
            }
        }
        return self.incomingPinRootsCurrent(prepared, sequence, latest.sequence, roots, lookup, &event_frame, &point_frame, &root_frame, scratch.bytes[0..change_capacity]);
    }
    fn projectedQueryPins(self: Store, prepared: PreparedQuery, roots: std.StringHashMapUnmanaged(void), lookup: AliasLookup, starts: *std.StringHashMapUnmanaged(void)) !bool {
        if (prepared.profile_id.len != 64) return false;
        const root = try self.pinProjectionRoot(prepared.template, prepared.profile_id);
        const raw = (try self.backend.get(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ root, "state" }))) orelse return false;
        const state = try parse(PinProjection, self.a, raw);
        if (state.format != pin_projection_format) return false;
        if (!(state.phase == .ready and std.mem.eql(u8, &state.graph, &prepared.graph_id)) and !try self.pinRootsCurrent(prepared, state, roots, lookup)) return false;
        var iterator = roots.keyIterator();
        while (iterator.next()) |id| {
            const prefix = try std.fmt.allocPrint(self.a, "{s}member/{s}/", .{ root, try hash(self.a, id.*) });
            var after: ?[]const u8 = null;
            while (true) {
                const page = try self.backend.scan(self.backend.ptr, self.a, prefix, after, 128);
                for (page.rows) |row| {
                    const member = try parse(PinMember, self.a, row.value);
                    if (!std.mem.eql(u8, member.root, id.*)) return error.InvalidCatalogRecord;
                    try starts.put(self.a, member.key, {});
                }
                after = page.next;
                if (after == null) break;
            }
        }
        return true;
    }
    fn staticQueryPins(self: Store, template: []const u8, lookup: AliasLookup, roots: std.StringHashMapUnmanaged(void), starts: *std.StringHashMapUnmanaged(void)) !void {
        const scratch = try query_context.RoutingScratch.init();
        defer scratch.deinit();
        var frame: std.heap.FixedBufferAllocator = .init(scratch.bytes);
        var temporary = self;
        temporary.a = frame.allocator();
        var canonical: QueryCanonical = .{ .store = temporary, .template = template, .lookup = lookup, .static_only = true, .frame = &frame };
        var iterator = roots.keyIterator();
        while (iterator.next()) |root| try self.queryPinNodes(template, root.*, &canonical, roots, starts, null);
    }
    fn queryIndexedPins(self: Store, h: Head, prepared: PreparedQuery, seeds: []const []const u8, lookup: AliasLookup, include_configured: bool) !Snapshot {
        const template = h.key_template;
        const pin_ready = try self.backend.get(self.backend.ptr, self.a, try self.pinReadyPath(template));
        if (pin_ready) |raw| if ((try parse(PinReady, self.a, raw)).endpoints == 0) return (try self.queryForwardIndexed(template, seeds, lookup, include_configured)).?;
        const changes = try self.hasQueryOverrides(template);
        var canonical: QueryCanonical = .{ .store = self, .template = template, .lookup = lookup, .static_only = !changes };
        var roots: std.StringHashMapUnmanaged(void) = .empty;
        for (seeds) |seed| try roots.put(self.a, try canonical.canonical(seed), {});
        var starts: std.StringHashMapUnmanaged(void) = .empty;
        var root_iterator = roots.keyIterator();
        while (root_iterator.next()) |root| try starts.put(self.a, root.*, {});
        var pairs: []const c.Pair = &.{};
        if (pin_ready != null) {
            // A pin can reach a configured path only through a profile node
            // with nonzero durable native pin weight. Join those two sparse
            // indexes directly; unrelated native overrides never participate.
            if (try self.projectedQueryPins(prepared, roots, lookup, &starts)) {
                // Static and curated graphs share complete root postings.
            } else if (!changes) {
                try self.staticQueryPins(template, lookup, roots, &starts);
            } else if (try self.queryPinMembership(prepared, lookup, changes)) |membership| {
                defer membership.release();
                var iterator = roots.keyIterator();
                while (iterator.next()) |root| for (membership.context.routesFor(root.*)) |member| try starts.put(self.a, try self.a.dupe(u8, member.component), {});
            } else {
                // Cache admission is optional. Stream the same exact join in
                // reusable scratch, retaining only selected endpoints.
                const scratch = try query_context.RoutingScratch.init();
                defer scratch.deinit();
                var frame: std.heap.FixedBufferAllocator = .init(scratch.bytes[0 .. 6 * 1024 * 1024]);
                var memo: std.heap.FixedBufferAllocator = .init(scratch.bytes[6 * 1024 * 1024 ..]);
                var temporary = self;
                temporary.a = frame.allocator();
                var streaming: QueryCanonical = .{ .store = temporary, .template = template, .lookup = lookup, .static_only = !changes, .frame = &frame, .memo_buffer = &memo };
                try self.queryPinNodes(template, null, &streaming, roots, &starts, null);
            }
            const selected = try self.a.alloc([]const u8, starts.count());
            var iterator = starts.keyIterator();
            for (selected) |*id| id.* = iterator.next().?.*;
            pairs = (try self.pinnedQuery(template, selected, &.{}, (Values{}).snapshot(h))).namespace.state.distinct;
        } else {
            // Released read-only files lack propagated pin postings until a
            // writer adopts them. Stream pairs, without reverse-alias walks.
            var legacy: std.ArrayList(c.Pair) = .empty;
            var after: ?[]const u8 = null;
            while (true) {
                const page = try self.backend.scan(self.backend.ptr, self.a, try self.records(template, .distinct), after, 128);
                for (page.rows) |row| {
                    const pair = try parse(c.Pair, self.a, row.value);
                    if (roots.contains(try canonical.canonical(pair.from)) or roots.contains(try canonical.canonical(pair.to))) try legacy.append(self.a, pair);
                }
                after = page.next;
                if (after == null) break;
            }
            pairs = legacy.items;
        }
        var selected: std.ArrayList([]const u8) = .empty;
        try selected.appendSlice(self.a, seeds);
        for (pairs) |pair| try selected.appendSlice(self.a, &.{ pair.from, pair.to });
        var result = (try self.queryForwardIndexed(template, selected.items, lookup, include_configured)).?;
        result.namespace.state.distinct = pairs;
        return result;
    }
    pub fn queryPrepared(self: Store, prepared: PreparedQuery, seeds: []const []const u8, texts: []const []const u8, distinct: bool) !Snapshot {
        return self.queryPreparedInternal(prepared, seeds, texts, distinct, false);
    }
    pub fn queryExecutionPrepared(self: Store, prepared: PreparedQuery, seeds: []const []const u8, texts: []const []const u8, distinct: bool) !Snapshot {
        return self.queryPreparedInternal(prepared, seeds, texts, distinct, true);
    }
    fn queryPreparedInternal(self: Store, prepared: PreparedQuery, seeds: []const []const u8, texts: []const []const u8, distinct: bool, include_configured: bool) !Snapshot {
        if (distinct and !prepared.distinct) return error.InvalidResolverCurationRequest;
        const h = (try self.head(prepared.template)) orelse return error.ResolverCurationConflict;
        if (h.revision != prepared.stamp.revision or !std.mem.eql(u8, &h.fingerprint, &prepared.stamp.fingerprint)) return error.ResolverCurationConflict;
        if (seeds.len == 0) {
            var snapshot = (try self.textPolicy(prepared.template, texts)).?;
            snapshot.complete_aliases = include_configured;
            return snapshot;
        }
        if (prepared.indexed) {
            var local: StaticQueryLookup = .{ .context = if (prepared.lease) |lease| lease.context else null };
            const lookup = prepared.lookup orelse AliasLookup{ .ptr = &local, .expand = StaticQueryLookup.expand, .node = StaticQueryLookup.node, .seek_node = StaticQueryLookup.seekNode };
            var snapshot = if (distinct and prepared.pin_discovery) try self.queryIndexedPins(h, prepared, seeds, lookup, include_configured) else (try self.queryForwardIndexed(prepared.template, seeds, lookup, include_configured)).?;
            try self.appendTextPolicy(prepared.template, texts, &snapshot);
            return snapshot;
        }
        const context = prepared.lease.?.context;
        var selected: std.ArrayList([]const u8) = .empty;
        try selected.appendSlice(self.a, seeds);
        var values: Values = .{};
        var pairs: std.StringHashMapUnmanaged(void) = .empty;
        if (distinct and context.composed and seeds.len != 0) for (seeds) |seed| {
            if (context.pins.get(context.redirects.canonical(seed))) |incident| for (incident.items) |pair| {
                if ((try pairs.getOrPut(self.a, try c.pairKey(self.a, pair.from, pair.to))).found_existing) continue;
                // No response string may borrow an evictable cache lease.
                const owned = try parse(c.Pair, self.a, try encode(self.a, pair));
                try values.distinct.append(self.a, owned);
                try selected.appendSlice(self.a, &.{ owned.from, owned.to });
            };
        };
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (selected.items) |seed| {
            var id = seed;
            while (true) {
                if ((try seen.getOrPut(self.a, id)).found_existing) break;
                if (seen.count() > 16384) return error.ResolverCurationLimitExceeded;
                inline for (.{ Kind.suppressed, Kind.suppression, Kind.redirect_origin }) |kind| if (try self.backend.get(self.backend.ptr, self.a, try self.record(prepared.template, kind, id))) |raw| try values.append(self.a, kind, raw);
                const native_target = if (context.composed) context.native_targets.get(id) else if (try self.get(resolver.KeyAlias, prepared.template, .redirect, id)) |alias| alias.target else null;
                if (native_target) |target| {
                    const owned: resolver.KeyAlias = .{ .key = try self.a.dupe(u8, id), .target = try self.a.dupe(u8, target) };
                    try values.aliases.append(self.a, owned);
                    id = owned.target;
                    continue;
                }
                const removed = if (context.composed) context.removed.contains(id) else try self.backend.get(self.backend.ptr, self.a, try self.record(prepared.template, .removed_alias, id)) != null;
                if (removed) {
                    try values.removed.append(self.a, try self.a.dupe(u8, id));
                    break;
                }
                const next = context.static_targets.get(id) orelse break;
                if (include_configured) try values.aliases.append(self.a, .{ .key = try self.a.dupe(u8, id), .target = try self.a.dupe(u8, next) });
                id = next;
            }
        }
        var snapshot = values.snapshot(h);
        snapshot.complete_aliases = include_configured;
        try self.appendTextPolicy(prepared.template, texts, &snapshot);
        return snapshot;
    }
    pub fn queryPolicy(self: Store, template: []const u8, seeds: []const []const u8, texts: []const []const u8, static_aliases: []const resolver.KeyAlias, distinct: bool) !?Snapshot {
        if (seeds.len == 0) return self.textPolicy(template, texts);
        if (static_aliases.len != 0) {
            var loader: InlineQueryProfile = .{ .aliases = static_aliases };
            const id = try (static_profiles.Definition{ .key_template = template, .aliases = static_aliases }).identity(self.a);
            const prepared = (try self.prepareQuery(template, .{ .id = id, .ptr = &loader, .load = InlineQueryProfile.load, .estimated_bytes = loader.estimate() }, distinct)) orelse return null;
            defer prepared.release();
            return try self.queryPrepared(prepared, seeds, texts, distinct);
        }
        var snapshot = (try self.queryForward(template, seeds, &.{}, 16384)) orelse return null;
        if (distinct) {
            if (try self.backend.get(self.backend.ptr, self.a, try self.pinReadyPath(template))) |raw_ready| {
                if ((try parse(PinReady, self.a, raw_ready)).endpoints != 0) snapshot = try self.pinnedQuery(template, seeds, &.{}, snapshot);
            } else if ((try self.backend.scan(self.backend.ptr, self.a, try self.records(template, .distinct), null, 1)).rows.len != 0)
                snapshot = (try self.component(template, seeds)).?;
        }
        try self.appendTextPolicy(template, texts, &snapshot);
        return snapshot;
    }
    pub fn queryExecutionPolicy(self: Store, template: []const u8, seeds: []const []const u8, texts: []const []const u8, static_aliases: []const resolver.KeyAlias, distinct: bool) !?Snapshot {
        if (static_aliases.len != 0 and seeds.len != 0) {
            var loader: InlineQueryProfile = .{ .aliases = static_aliases };
            const id = try (static_profiles.Definition{ .key_template = template, .aliases = static_aliases }).identity(self.a);
            const prepared = (try self.prepareQuery(template, .{ .id = id, .ptr = &loader, .load = InlineQueryProfile.load, .estimated_bytes = loader.estimate() }, distinct)) orelse return null;
            defer prepared.release();
            return try self.queryExecutionPrepared(prepared, seeds, texts, distinct);
        }
        var snapshot = (try self.queryPolicy(template, seeds, texts, &.{}, distinct)) orelse return null;
        snapshot.complete_aliases = true;
        return snapshot;
    }
    fn queryForward(self: Store, template: []const u8, seeds: []const []const u8, static_aliases: []const resolver.KeyAlias, limit: usize) !?Snapshot {
        const h = try self.head(template) orelse return null;
        var edges: std.StringHashMapUnmanaged([]const u8) = .empty;
        for (static_aliases) |alias| try edges.put(self.a, alias.key, alias.target);
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        var values: Values = .{};
        for (seeds) |seed| {
            var id = seed;
            while (true) {
                if ((try seen.getOrPut(self.a, id)).found_existing) break;
                if (seen.count() > limit) return error.ResolverCurationLimitExceeded;
                inline for (.{ Kind.suppressed, Kind.suppression, Kind.redirect_origin, Kind.removed_alias }) |kind| if (try self.backend.get(self.backend.ptr, self.a, try self.record(template, kind, id))) |raw| try values.append(self.a, kind, raw);
                if (try self.get(resolver.KeyAlias, template, .redirect, id)) |alias| {
                    try values.aliases.append(self.a, alias);
                    id = alias.target;
                } else {
                    if (try self.get([]const u8, template, .removed_alias, id) != null) break;
                    id = edges.get(id) orelse break;
                }
            }
        }
        return values.snapshot(h);
    }
    fn pinnedQuery(self: Store, template: []const u8, seeds: []const []const u8, static_aliases: []const resolver.KeyAlias, initial: Snapshot) !Snapshot {
        const graph = initial;
        var native_keys: std.StringHashMapUnmanaged(void) = .empty;
        for (graph.namespace.merges) |alias| try native_keys.put(self.a, alias.key, {});
        const policy_index = try c.Index.init(self.a, "", try encode(self.a, graph.namespace.state));
        var effective: std.ArrayList(resolver.KeyAlias) = .empty;
        for (static_aliases) |alias| if (!native_keys.contains(alias.key) and !policy_index.removed_aliases.contains(alias.key)) try effective.append(self.a, alias);
        try effective.appendSlice(self.a, graph.namespace.merges);
        const redirects = try c.RedirectIndex.init(self.a, effective.items);
        var canonical: std.StringHashMapUnmanaged(void) = .empty;
        var pending: std.ArrayList([]const u8) = .empty;
        for (seeds) |seed| {
            const id = redirects.canonical(seed);
            if (!(try canonical.getOrPut(self.a, id)).found_existing) try pending.append(self.a, id);
        }
        var visited: std.StringHashMapUnmanaged(void) = .empty;
        var pairs: std.StringHashMapUnmanaged(c.Pair) = .empty;
        var offset: usize = 0;
        while (offset < pending.items.len) : (offset += 1) {
            const id = pending.items[offset];
            if ((try visited.getOrPut(self.a, id)).found_existing or try self.pinCount(template, id) == 0) continue;
            inline for (.{ "distinct-key", "pin-child" }) |kind| {
                var after: ?[]const u8 = null;
                while (true) {
                    const page = try self.backend.scan(self.backend.ptr, self.a, try self.adjacent(template, kind, id), after, 128);
                    for (page.rows) |row| {
                        if (comptime std.mem.eql(u8, kind, "pin-child")) try pending.append(self.a, try parse([]const u8, self.a, row.value)) else try pairs.put(self.a, row.key[row.key.len - 64 ..], try parse(c.Pair, self.a, row.value));
                    }
                    after = page.next;
                    if (after == null) break;
                }
            }
        }
        if (pairs.count() == 0) return initial;
        var selected: std.ArrayList([]const u8) = .empty;
        try selected.appendSlice(self.a, seeds);
        var values: std.ArrayList(c.Pair) = .empty;
        var it = pairs.valueIterator();
        while (it.next()) |pair| {
            try values.append(self.a, pair.*);
            try selected.appendSlice(self.a, &.{ pair.from, pair.to });
        }
        var result = (try self.queryForward(template, selected.items, static_aliases, 16384)).?;
        result.namespace.state.distinct = values.items;
        return result;
    }
    pub fn mentionPolicy(self: Store, template: []const u8, key: []const u8, text: []const u8, static_aliases: []const resolver.KeyAlias) !?Snapshot {
        return self.queryPolicy(template, &.{key}, &.{text}, static_aliases, false);
    }
    pub fn component(self: Store, template: []const u8, seeds: []const []const u8) !?Snapshot {
        return self.componentWithAliases(template, seeds, &.{});
    }
    pub fn componentWithAliases(self: Store, template: []const u8, seeds: []const []const u8, static_aliases: []const resolver.KeyAlias) !?Snapshot {
        return self.componentGraph(template, seeds, static_aliases, null, true);
    }
    pub fn componentIndexed(self: Store, template: []const u8, seeds: []const []const u8, lookup: AliasLookup) !?Snapshot {
        return self.componentGraph(template, seeds, &.{}, lookup, true);
    }
    fn componentGraph(self: Store, template: []const u8, seeds: []const []const u8, static_aliases: []const resolver.KeyAlias, lookup: ?AliasLookup, include_cascades: bool) !?Snapshot {
        const Mask = struct {
            store: Store,
            template: []const u8,
            cache: std.StringHashMapUnmanaged(bool) = .empty,
            fn enabled(ptr: *anyopaque, a: A, id: []const u8) !bool {
                const context: *@This() = @ptrCast(@alignCast(ptr));
                if (context.cache.get(id)) |value| return value;
                const active = try context.store.get(resolver.KeyAlias, context.template, .redirect, id) == null and try context.store.backend.get(context.store.backend.ptr, a, try context.store.record(context.template, .removed_alias, id)) == null;
                try context.cache.put(a, try a.dupe(u8, id), active);
                return active;
            }
        };
        var masks: Mask = .{ .store = self, .template = template };
        const mask: EdgeMask = .{ .ptr = &masks, .enabled = Mask.enabled };
        var forward: std.StringHashMapUnmanaged(std.ArrayList([]const u8)) = .empty;
        var reverse: std.StringHashMapUnmanaged(std.ArrayList([]const u8)) = .empty;
        for (static_aliases) |alias| {
            if (!try mask.enabled(mask.ptr, self.a, alias.key)) continue;
            const from = try forward.getOrPut(self.a, alias.key);
            if (!from.found_existing) from.value_ptr.* = .empty;
            try from.value_ptr.append(self.a, alias.target);
            const to = try reverse.getOrPut(self.a, alias.target);
            if (!to.found_existing) to.value_ptr.* = .empty;
            try to.value_ptr.append(self.a, alias.key);
        }
        const h = try self.head(template) orelse return null;
        var values: Values = .{};
        var pending: std.ArrayList([]const u8) = .empty;
        try pending.appendSlice(self.a, seeds);
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        var pairs: std.StringHashMapUnmanaged(void) = .empty;
        var offset: usize = 0;
        while (offset < pending.items.len) : (offset += 1) {
            const id = pending.items[offset];
            if ((try seen.getOrPut(self.a, id)).found_existing) continue;
            if (seen.count() > 16384) return error.ResolverCurationLimitExceeded;
            if (lookup) |indexed| for (try indexed.expand(indexed.ptr, self.a, id, false, mask)) |edge| {
                try pending.append(self.a, edge.key);
                try pending.append(self.a, edge.target);
            };
            if (forward.get(id)) |edges| try pending.appendSlice(self.a, edges.items);
            if (reverse.get(id)) |edges| try pending.appendSlice(self.a, edges.items);
            if (try self.get(resolver.KeyAlias, template, .redirect, id)) |alias| {
                try values.aliases.append(self.a, alias);
                try pending.append(self.a, alias.target);
            }
            inline for (.{ Kind.suppressed, Kind.suppression, Kind.redirect_origin, Kind.removed_alias }) |kind| {
                if (try self.backend.get(self.backend.ptr, self.a, try self.record(template, kind, id))) |raw| try values.append(self.a, kind, raw);
            }
            inline for (.{ "incoming", "cascade", "distinct-key" }) |name| {
                const enabled = if (comptime std.mem.eql(u8, name, "cascade")) include_cascades else true;
                if (enabled) {
                    const prefix = try self.adjacent(template, name, id);
                    var after: ?[]const u8 = null;
                    while (true) {
                        const page = try self.backend.scan(self.backend.ptr, self.a, prefix, after, 128);
                        for (page.rows) |row| {
                            if (comptime std.mem.eql(u8, name, "distinct-key")) {
                                if (!(try pairs.getOrPut(self.a, row.key[row.key.len - 64 ..])).found_existing) try values.distinct.append(self.a, try parse(c.Pair, self.a, row.value));
                            } else try pending.append(self.a, try parse([]const u8, self.a, row.value));
                        }
                        after = page.next;
                        if (after == null) break;
                    }
                }
            }
        }
        // Canonicalize incident distinctions without expanding unrelated
        // reverse components or traversing the entire distinct graph.
        for (values.distinct.items) |pair| for ([_][]const u8{ pair.from, pair.to }) |endpoint| {
            var paths: std.ArrayList([]const u8) = .empty;
            try paths.append(self.a, endpoint);
            var path_offset: usize = 0;
            while (path_offset < paths.items.len) : (path_offset += 1) {
                const id = paths.items[path_offset];
                if ((try seen.getOrPut(self.a, id)).found_existing) continue;
                if (seen.count() > 16384) return error.ResolverCurationLimitExceeded;
                inline for (.{ Kind.suppressed, Kind.suppression, Kind.redirect_origin, Kind.removed_alias }) |kind| if (try self.backend.get(self.backend.ptr, self.a, try self.record(template, kind, id))) |raw| try values.append(self.a, kind, raw);
                if (lookup) |indexed| for (try indexed.expand(indexed.ptr, self.a, id, true, mask)) |edge| try paths.append(self.a, edge.target);
                if (forward.get(id)) |edges| try paths.appendSlice(self.a, edges.items);
                if (try self.get(resolver.KeyAlias, template, .redirect, id)) |alias| {
                    try values.aliases.append(self.a, alias);
                    try paths.append(self.a, alias.target);
                }
            }
        };
        return values.snapshot(h);
    }
    fn indexRecord(self: Store, template: []const u8, kind: Kind, id: []const u8, raw: []const u8, add: bool) !void {
        var paths: std.ArrayList([]const u8) = .empty;
        var value: []const u8 = raw;
        switch (kind) {
            .redirect => {
                const alias = try parse(resolver.KeyAlias, self.a, raw);
                try paths.append(self.a, try std.mem.concat(self.a, u8, &.{ try self.adjacent(template, "incoming", alias.target), try hash(self.a, alias.key) }));
                value = try encode(self.a, alias.key);
            },
            .distinct => {
                const pair = try parse(c.Pair, self.a, raw);
                for ([_][]const u8{ pair.from, pair.to }) |key| try paths.append(self.a, try std.mem.concat(self.a, u8, &.{ try self.adjacent(template, "distinct-key", key), try hash(self.a, id) }));
            },
            .suppression => {
                const entry = try parse(c.Entry, self.a, raw);
                if (entry.origin.cascade_from) |from| {
                    try paths.append(self.a, try std.mem.concat(self.a, u8, &.{ try self.adjacent(template, "cascade", from), try hash(self.a, entry.key) }));
                    value = try encode(self.a, entry.key);
                }
            },
            else => {},
        }
        for (paths.items) |path| if (add) try self.backend.put(self.backend.ptr, self.a, path, value) else try self.backend.remove(self.backend.ptr, self.a, path);
    }
    pub fn queueHead(self: Store) !QueueHead {
        return if (try self.backend.get(self.backend.ptr, self.a, try self.keyPath("queue/head"))) |raw| try parse(QueueHead, self.a, raw) else .{};
    }
    fn proposalPath(self: Store, id: []const u8) ![]const u8 {
        if (id.len != 64) return error.InvalidResolverCurationRequest;
        for (id) |byte| if (!std.ascii.isHex(byte)) return error.InvalidResolverCurationRequest;
        return self.keyPath(try std.mem.concat(self.a, u8, &.{ "queue/proposal/", id }));
    }
    fn proposalHeaderPath(self: Store, id: []const u8) ![]const u8 {
        _ = try self.proposalPath(id);
        return self.keyPath(try std.mem.concat(self.a, u8, &.{ "queue/header/", id }));
    }
    fn proposalRecord(self: Store, id: []const u8) !?ProposalHeader {
        if (try self.backend.get(self.backend.ptr, self.a, try self.proposalHeaderPath(id))) |raw| {
            const header = try parse(ProposalHeader, self.a, raw);
            if (header.version != 1 or header.proposal.evidence != null or !std.mem.eql(u8, id, header.proposal.id)) return error.InvalidCatalogRecord;
            return header;
        }
        // Legacy rows acquire their compact header on first mutation. Never
        // scan/migrate an unrelated queue, including unbounded memory queues.
        return if (try self.backend.get(self.backend.ptr, self.a, try self.proposalPath(id))) |raw| try proposalHeaderValue(self.a, try parse(c.Proposal, self.a, raw)) else null;
    }
    pub fn proposalHeader(self: Store, id: []const u8) !?c.Proposal {
        return if (try self.proposalRecord(id)) |header| header.proposal else null;
    }
    pub fn proposal(self: Store, id: []const u8) !?c.Proposal {
        const header = try self.proposalRecord(id) orelse return null;
        const raw = try self.backend.get(self.backend.ptr, self.a, try self.proposalPath(id)) orelse return error.InvalidCatalogRecord;
        return try self.joinProposal(try parse(c.Proposal, self.a, raw), header);
    }
    fn joinProposal(self: Store, body: c.Proposal, header: ProposalHeader) !c.Proposal {
        if (header.version != 1 or header.proposal.evidence != null or !std.mem.eql(u8, body.id, header.proposal.id)) return error.InvalidCatalogRecord;
        var value = header.proposal;
        value.evidence = body.evidence;
        if (!std.mem.eql(u8, &header.evidence_hash, &(try evidenceHash(self.a, value.evidence)))) return error.InvalidCatalogRecord;
        return value;
    }
    fn scannedProposal(self: Store, raw: []const u8) !c.Proposal {
        const body = try parse(c.Proposal, self.a, raw);
        const header = try self.backend.get(self.backend.ptr, self.a, try self.proposalHeaderPath(body.id)) orelse return body;
        return self.joinProposal(body, try parse(ProposalHeader, self.a, header));
    }
    fn nextProposalRecord(self: Store, patch: QueuePatch, value: c.Proposal, previous: ?ProposalHeader) !ProposalHeader {
        if (!patch.retain_evidence) return proposalHeaderValue(self.a, value);
        const old = previous orelse return error.ResolverCurationConflict;
        if (value.evidence != null) return error.InvalidResolverCurationRequest;
        var unchanged = value;
        unchanged.state = old.proposal.state;
        unchanged.decided = old.proposal.decided;
        if (!std.mem.eql(u8, try encode(self.a, unchanged), try encode(self.a, old.proposal))) return error.ResolverCurationConflict;
        return .{ .proposal = value, .evidence_hash = old.evidence_hash, .capacity = try std.math.sub(u64, try std.math.add(u64, old.capacity, c.proposalCapacityBytes(.{ .proposals = &.{value} })), c.proposalCapacityBytes(.{ .proposals = &.{old.proposal} })) };
    }
    fn proposalIndexes(self: Store, value: c.Proposal) ![]const []const u8 {
        const result = try self.a.alloc([]const u8, if (value.state == .open) (if (value.into != null) @as(usize, 5) else 4) else 3);
        result[0] = try self.keyPath(try std.fmt.allocPrint(self.a, "queue/state/{s}/{s}", .{ @tagName(value.state), value.id }));
        result[1] = try self.keyPath(try std.fmt.allocPrint(self.a, "queue/action/{s}/{s}", .{ @tagName(value.action), value.id }));
        result[2] = try self.keyPath(try std.fmt.allocPrint(self.a, "queue/filter/{s}/{s}/{s}", .{ @tagName(value.state), @tagName(value.action), value.id }));
        if (value.state == .open) {
            result[3] = try self.keyPath(try std.fmt.allocPrint(self.a, "queue/key/{s}/{s}", .{ try hash(self.a, value.key), value.id }));
            if (value.into) |target| result[4] = try self.keyPath(try std.fmt.allocPrint(self.a, "queue/key/{s}/{s}", .{ try hash(self.a, target), value.id }));
        }
        return result;
    }
    /// Durable effect classification, separate from actor provenance/revision.
    /// It commits with a plain rejection and lets receipt replay avoid all
    /// consumer work without mistaking a rejected distinct pin for queue-only.
    pub fn markQueueOnly(self: Store, id: []const u8) !void {
        const value = try self.proposalHeader(id) orelse return error.NotFound;
        if (value.state != .rejected) return error.ResolverCurationConflict;
        try self.backend.put(self.backend.ptr, self.a, try self.keyPath(try std.fmt.allocPrint(self.a, "queue/queue-only/{s}", .{id})), try encode(self.a, id));
    }
    pub fn queueOnly(self: Store, id: []const u8) !bool {
        const raw = try self.backend.get(self.backend.ptr, self.a, try self.keyPath(try std.fmt.allocPrint(self.a, "queue/queue-only/{s}", .{id}))) orelse return false;
        if (!std.mem.eql(u8, try parse([]const u8, self.a, raw), id)) return error.InvalidMetadataBatch;
        return true;
    }
    pub fn proposalPage(self: Store, state: ?c.ProposalState, action: ?c.EntityAction, after: ?[]const u8, limit: u32) !c.ProposalPage {
        if (limit == 0 or limit > 256) return error.InvalidResolverCurationRequest;
        const suffix = if (state) |v| if (action) |w| try std.fmt.allocPrint(self.a, "queue/filter/{s}/{s}/", .{ @tagName(v), @tagName(w) }) else try std.fmt.allocPrint(self.a, "queue/state/{s}/", .{@tagName(v)}) else if (action) |v| try std.fmt.allocPrint(self.a, "queue/action/{s}/", .{@tagName(v)}) else "queue/proposal/";
        const prefix = try self.keyPath(suffix);
        const page = try self.backend.scan(self.backend.ptr, self.a, prefix, if (after) |id| try std.mem.concat(self.a, u8, &.{ prefix, id }) else null, limit + 1);
        var values: std.ArrayList(c.Proposal) = .empty;
        for (page.rows[0..@min(limit, page.rows.len)]) |row| {
            const value = if (state == null and action == null) try self.scannedProposal(row.value) else (try self.proposal(row.key[row.key.len - 64 ..])) orelse return error.InvalidCatalogRecord;
            try values.append(self.a, value);
        }
        return .{ .proposals = values.items, .next_cursor = if (page.rows.len > limit) values.items[values.items.len - 1].id else null };
    }
    pub fn proposalsForKeys(self: Store, keys: []const []const u8) !c.State {
        return self.proposalsForKeysMode(keys, false);
    }
    pub fn proposalHeadersForKeys(self: Store, keys: []const []const u8) !c.State {
        return self.proposalsForKeysMode(keys, true);
    }
    fn proposalsForKeysMode(self: Store, keys: []const []const u8, headers_only: bool) !c.State {
        const h = try self.queueHead();
        var values: std.ArrayList(c.Proposal) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        var seen_keys: std.StringHashMapUnmanaged(void) = .empty;
        for (keys) |key| {
            if ((try seen_keys.getOrPut(self.a, key)).found_existing) continue;
            const prefix = try self.keyPath(try std.fmt.allocPrint(self.a, "queue/key/{s}/", .{try hash(self.a, key)}));
            var after: ?[]const u8 = null;
            while (true) {
                if (comptime @import("builtin").is_test) _ = test_proposal_key_scans.fetchAdd(1, .monotonic);
                const page = try self.backend.scan(self.backend.ptr, self.a, prefix, after, 128);
                for (page.rows) |row| {
                    const id = row.key[row.key.len - 64 ..];
                    if ((try seen.getOrPut(self.a, id)).found_existing) continue;
                    const value = (if (headers_only) try self.proposalHeader(id) else try self.proposal(id)) orelse return error.InvalidCatalogRecord;
                    if (value.state == .open) try values.append(self.a, value);
                }
                after = page.next;
                if (after == null) break;
            }
        }
        return .{ .receipt_scope = h.receipt_scope, .proposals = values.items };
    }
    const Ownership = @import("resolver_ownership_index.zig");
    pub const OwnershipProgress = struct { revision: u64, after: ?[]const u8 = null, complete: bool = false };
    fn ownershipProgress(self: Store) !?OwnershipProgress {
        const raw = try self.backend.get(self.backend.ptr, self.a, try self.keyPath("queue/ownership-progress")) orelse return null;
        return try parse(OwnershipProgress, self.a, raw);
    }
    fn ownershipIndex(self: Store, direction: []const u8) !Ownership.Index {
        return .{ .a = self.a, .backend = self.backend, .root = try self.keyPath(try std.fmt.allocPrint(self.a, "queue/ownership/{s}/", .{direction})) };
    }
    fn indexOwnedKey(self: Store, key: []const u8) !void {
        const member = try self.keyPath(try std.fmt.allocPrint(self.a, "queue/ownership/member/{s}", .{try hash(self.a, key)}));
        if (try self.backend.get(self.backend.ptr, self.a, member) != null) return;
        try (try self.ownershipIndex("forward")).insert(try Ownership.encodedKey(self.a, key, false), key);
        try (try self.ownershipIndex("reverse")).insert(try Ownership.encodedKey(self.a, key, true), key);
        try self.backend.put(self.backend.ptr, self.a, member, try encode(self.a, key));
    }
    pub fn ownershipIndexed(self: Store) !bool {
        const h = try self.queueHead();
        if (h.revision == 0) return true;
        const progress = (try self.ownershipProgress()) orelse return false;
        return progress.complete and progress.revision == h.revision;
    }
    /// Owner work only: the watermark and index afterimages share the queue's
    /// writer transaction. Concurrent new writers enroll new keys immediately;
    /// an older writer advancing only the queue revision resets the cut safely.
    pub fn advanceOwnershipIndex(self: Store, limit: u32) !bool {
        if (limit == 0 or limit > 4096) return error.InvalidResolverCurationRequest;
        const h = try self.queueHead();
        var progress = (try self.ownershipProgress()) orelse OwnershipProgress{ .revision = h.revision };
        if (progress.revision != h.revision) progress = .{ .revision = h.revision };
        if (progress.complete) return true;
        const prefix = try self.keyPath("queue/owned/");
        const after = if (progress.after) |id| try std.mem.concat(self.a, u8, &.{ prefix, id }) else null;
        const page = try self.backend.scan(self.backend.ptr, self.a, prefix, after, limit);
        for (page.rows) |row| try self.indexOwnedKey(try parse([]const u8, self.a, row.value));
        progress.after = if (page.next) |key| try self.a.dupe(u8, key[prefix.len..]) else null;
        progress.complete = page.next == null;
        try self.backend.put(self.backend.ptr, self.a, try self.keyPath("queue/ownership-progress"), try encode(self.a, progress));
        return progress.complete;
    }
    pub fn ownsKeyTemplate(self: Store, template: []const u8) !bool {
        if (try self.head(template) != null) return true;
        const first = std.mem.indexOf(u8, template, "{{") orelse {
            return try self.backend.get(self.backend.ptr, self.a, try self.keyPath(try std.fmt.allocPrint(self.a, "queue/owned/{s}", .{try hash(self.a, template)}))) != null;
        };
        if (try self.ownershipIndexed()) {
            const last = std.mem.lastIndexOf(u8, template, "}}") orelse return false;
            const forward = try self.ownershipIndex("forward");
            const prefix_cut = (try forward.prefix(try Ownership.encodedKey(self.a, template[0..first], false))) orelse return false;
            const suffix = try Ownership.encodedKey(self.a, template[last + 2 ..], true);
            const reverse = try self.ownershipIndex("reverse");
            const tail = (try reverse.prefix(suffix)) orelse return false;
            if (first == 0 and suffix.len == 0) return true;
            return if (prefix_cut.refs <= tail.refs) forward.matches(prefix_cut, template) else reverse.matches(tail, template);
        }
        // Read-only old snapshots retain ownership until bounded owner work
        // publishes the complete queue-revision-fenced index.
        const prefix = try self.keyPath("queue/owned/");
        var after: ?[]const u8 = null;
        while (true) {
            const page = try self.backend.scan(self.backend.ptr, self.a, prefix, after, 128);
            for (page.rows) |row| if (policy.keyInNamespace(template, try parse([]const u8, self.a, row.value))) return true;
            after = page.next;
            if (after == null) break;
        }
        return false;
    }
    pub fn preflightQueue(self: Store, patch: QueuePatch) !void {
        const h = try self.queueHead();
        if (h.revision != patch.expected_revision or patch.revision <= h.revision) return error.ResolverCurationConflict;
        if (h.revision != 0 and patch.receipt_scope != null and !c.sameReceiptScope(h.receipt_scope, patch.receipt_scope)) return error.ResolverCurationConflict;
        var bytes = h.bytes;
        var count = h.count;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (patch.proposals) |value| {
            const path = try self.proposalPath(value.id);
            if ((try seen.getOrPut(self.a, path)).found_existing) return error.InvalidResolverCurationRequest;
            _ = try c.parseState(self.a, try encode(self.a, c.State{ .receipt_scope = h.receipt_scope, .proposals = &.{value} }));
            const previous = try self.proposalRecord(value.id);
            const next = try self.nextProposalRecord(patch, value, previous);
            if (previous) |old| {
                if (old.proposal.state != .open and !std.mem.eql(u8, try encode(self.a, old), try encode(self.a, next))) return error.ResolverCurationConflict;
                bytes = try std.math.sub(u64, bytes, old.capacity);
            } else count = try std.math.add(u32, count, 1);
            bytes = try std.math.add(u64, bytes, next.capacity);
        }
        if (self.max_queue_bytes) |maximum| if (bytes > maximum) return error.ResolverCurationLimitExceeded;
        if (self.max_queue_count) |maximum| if (count > maximum) return error.ResolverCurationLimitExceeded;
    }
    pub fn applyQueue(self: Store, patch: QueuePatch) !void {
        try self.preflightQueue(patch);
        var h = try self.queueHead();
        const old_revision = h.revision;
        var ownership = (try self.ownershipProgress()) orelse if (old_revision == 0) OwnershipProgress{ .revision = 0, .complete = true } else null;
        if (h.revision == 0) h.receipt_scope = patch.receipt_scope;
        for (patch.proposals) |value| {
            const path = try self.proposalPath(value.id);
            const previous = try self.proposalRecord(value.id);
            const next = try self.nextProposalRecord(patch, value, previous);
            if (previous) |old| {
                h.bytes -= old.capacity;
                for (try self.proposalIndexes(old.proposal)) |index| try self.backend.remove(self.backend.ptr, self.a, index);
            } else h.count += 1;
            h.bytes += next.capacity;
            // The original body owns immutable evidence. Decisions update the
            // small header and indexes; public reads join evidence on demand.
            if (previous == null or !std.mem.eql(u8, &previous.?.evidence_hash, &next.evidence_hash)) try self.backend.put(self.backend.ptr, self.a, path, try encode(self.a, value));
            try self.backend.put(self.backend.ptr, self.a, try self.proposalHeaderPath(value.id), try encode(self.a, next));
            try self.backend.put(self.backend.ptr, self.a, try self.keyPath(try std.fmt.allocPrint(self.a, "queue/owned/{s}", .{try hash(self.a, value.key)})), try encode(self.a, value.key));
            try self.indexOwnedKey(value.key);
            for (try self.proposalIndexes(value)) |index| try self.backend.put(self.backend.ptr, self.a, index, try encode(self.a, value.id));
        }
        h.revision = patch.revision;
        if (ownership) |*progress| if (progress.revision == old_revision) {
            progress.revision = h.revision;
            try self.backend.put(self.backend.ptr, self.a, try self.keyPath("queue/ownership-progress"), try encode(self.a, progress.*));
        };
        try self.backend.put(self.backend.ptr, self.a, try self.keyPath("queue/head"), try encode(self.a, h));
    }
    /// One-time import preserves the legacy revision, including revision zero.
    /// Callers stage this in the same owner transaction as their first edit.
    pub fn adoptNamespace(self: Store, namespace: policy.Namespace) !void {
        if (try self.head(namespace.key_template) != null) return error.ResolverCurationConflict;
        const rows = try flatten(self.a, namespace);
        var h: Head = .{ .key_template = namespace.key_template, .revision = namespace.revision, .adoption_revision = namespace.adoption_revision, .adoption_standalone = namespace.adoption_standalone, .min_text_chars = namespace.state.suppress.min_text_chars };
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        _ = try c.RedirectIndex.init(self.a, namespace.merges);
        _ = try c.Index.init(self.a, "", try encode(self.a, namespace.state));
        for (rows) |row| {
            try self.validateRecord(h.key_template, row.kind, row.value);
            const path = try self.record(h.key_template, row.kind, row.id);
            if ((try seen.getOrPut(self.a, path)).found_existing) return error.InvalidResolverCurationRequest;
            h.bytes = try std.math.add(u64, h.bytes, row.value.len);
        }
        if (self.max_namespace_bytes) |maximum| if (h.bytes > maximum) return error.ResolverCurationLimitExceeded;
        @import("antfly_hash").Sha256.hash(try encode(self.a, namespace), &h.fingerprint, .{});
        for (rows) |row| {
            try self.backend.put(self.backend.ptr, self.a, try self.record(h.key_template, row.kind, row.id), row.value);
            try self.indexRecord(h.key_template, row.kind, row.id, row.value, true);
        }
        if (namespace.state.distinct.len == 0) {
            try self.backend.put(self.backend.ptr, self.a, try self.pinReadyPath(h.key_template), try encode(self.a, PinReady{ .endpoints = 0 }));
        } else try self.ensurePinIndex(h.key_template);
        var graph_bytes: u64 = 0;
        for (rows) |row| if (graphKind(row.kind)) {
            graph_bytes += row.value.len;
        };
        try self.putQueryGraph(h, h.fingerprint, graph_bytes);
        try self.putRouting(h, h.fingerprint);
        try self.indexHead(h);
        try self.backend.put(self.backend.ptr, self.a, try self.headPath(h.key_template), try encode(self.a, h));
    }
    fn validateRecord(self: Store, template: []const u8, kind: Kind, raw: []const u8) !void {
        switch (kind) {
            .redirect => {
                const item = try parse(resolver.KeyAlias, self.a, raw);
                try c.validKey(item.key);
                try c.validKey(item.target);
                if (!policy.keyInNamespace(template, item.key) or !policy.keyInNamespace(template, item.target) or std.mem.eql(u8, item.key, item.target)) return error.InvalidResolverCurationRequest;
            },
            .distinct => {
                const item = try parse(c.Pair, self.a, raw);
                try c.validKey(item.from);
                try c.validKey(item.to);
                if (item.origin) |origin| try origin.validate();
                if (!policy.keyInNamespace(template, item.from) or !policy.keyInNamespace(template, item.to) or std.mem.eql(u8, item.from, item.to)) return error.InvalidResolverCurationRequest;
            },
            .suppression, .text_origin => {
                const item = try parse(c.Entry, self.a, raw);
                try c.validKey(item.key);
                try item.origin.validate();
                if (kind == .suppression and !policy.keyInNamespace(template, item.key)) return error.InvalidResolverCurationRequest;
                if (item.origin.cascade_from) |key| if (!policy.keyInNamespace(template, key)) return error.InvalidResolverCurationRequest;
            },
            .redirect_origin => {
                const item = try parse(c.RedirectOrigin, self.a, raw);
                try c.validKey(item.key);
                try c.validKey(item.target);
                try item.origin.validate();
                if (!policy.keyInNamespace(template, item.key) or !policy.keyInNamespace(template, item.target)) return error.InvalidResolverCurationRequest;
            },
            .suppressed, .removed_alias => {
                const key = try parse([]const u8, self.a, raw);
                try c.validKey(key);
                if (!policy.keyInNamespace(template, key)) return error.InvalidResolverCurationRequest;
            },
            .text => {},
        }
    }
    pub fn preflight(self: Store, patch: Patch) !void {
        const previous = try self.head(patch.next.key_template);
        if (patch.next.version != 1 or (if (previous) |h| h.revision else 0) != patch.expected_revision or patch.next.revision <= patch.expected_revision) return error.ResolverCurationConflict;
        var bytes: u64 = if (previous) |h| h.bytes else 0;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (patch.edits) |edit| {
            const path = try self.record(patch.next.key_template, edit.kind, edit.id);
            if ((try seen.getOrPut(self.a, path)).found_existing) return error.InvalidResolverCurationRequest;
            if (try self.backend.get(self.backend.ptr, self.a, path)) |old| bytes = try std.math.sub(u64, bytes, old.len);
            if (edit.value) |value_json| {
                const raw = try encode(self.a, value_json);
                try self.validateRecord(patch.next.key_template, edit.kind, raw);
                var value: Values = .{};
                try value.append(self.a, edit.kind, raw);
                const namespace = value.snapshot(patch.next).namespace;
                const entries = try flatten(self.a, namespace);
                if (entries.len != 1 or !std.mem.eql(u8, entries[0].id, edit.id)) return error.InvalidResolverCurationRequest;
                bytes = try std.math.add(u64, bytes, raw.len);
            }
        }
        if (self.max_namespace_bytes) |maximum| if (bytes > maximum) return error.ResolverCurationLimitExceeded;
    }
    pub fn apply(self: Store, patch: Patch) !void {
        var changes: std.StringHashMapUnmanaged(void) = .empty;
        var mutation = self;
        const enrolled = try self.backend.get(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ try self.pinEditRoot(patch.next.key_template), "head" })) != null;
        mutation.pin_changes = if (enrolled) &changes else null;
        try mutation.preflight(patch);
        const previous = try mutation.head(patch.next.key_template);
        if ((if (previous) |h| h.revision else 0) != patch.expected_revision or patch.next.revision <= patch.expected_revision) return error.ResolverCurationConflict;
        var graph = if (previous) |h| try mutation.queryGraph(h) else QueryGraph{ .head = .{ .revision = 0, .fingerprint = @splat(0) }, .fingerprint = @splat(0), .bytes = 0 };
        var routing_fingerprint = if (previous) |h| try mutation.routingFingerprint(h) else @as([32]u8, @splat(0));
        var routing_targets: std.ArrayList([]const u8) = .empty;
        var routing_edits: std.ArrayList(Edit) = .empty;
        for (patch.edits) |edit| if (edit.kind == .redirect or edit.kind == .removed_alias) {
            try routing_edits.append(mutation.a, edit);
            if (enrolled and edit.kind == .redirect) {
                if (try mutation.get(resolver.KeyAlias, patch.next.key_template, .redirect, edit.id)) |edge| try routing_targets.append(mutation.a, edge.target);
                if (edit.value) |value| try routing_targets.append(mutation.a, (try parse(resolver.KeyAlias, mutation.a, try encode(mutation.a, value))).target);
            }
        };
        if (routing_edits.items.len != 0) @import("antfly_hash").Sha256.hash(try encode(mutation.a, .{ routing_fingerprint, patch.next.key_template, routing_edits.items }), &routing_fingerprint, .{});
        var graph_edits: std.ArrayList(Edit) = .empty;
        for (patch.edits) |edit| if (graphKind(edit.kind)) {
            try graph_edits.append(mutation.a, edit);
            if (try mutation.backend.get(mutation.backend.ptr, mutation.a, try mutation.record(patch.next.key_template, edit.kind, edit.id))) |raw| graph.bytes = try std.math.sub(u64, graph.bytes, raw.len);
            if (edit.value) |value| graph.bytes = try std.math.add(u64, graph.bytes, (try encode(mutation.a, value)).len);
        };
        if (graph_edits.items.len != 0) @import("antfly_hash").Sha256.hash(try encode(mutation.a, .{ graph.fingerprint, patch.next.key_template, graph_edits.items }), &graph.fingerprint, .{});
        try mutation.ensurePinIndex(patch.next.key_template);
        var detached: std.StringHashMapUnmanaged(void) = .empty;
        var endpoints: std.StringHashMapUnmanaged(bool) = .empty;
        for (patch.edits) |edit| if (edit.kind == .distinct) {
            if (try mutation.get(c.Pair, patch.next.key_template, .distinct, edit.id)) |pair| for ([_][]const u8{ pair.from, pair.to }) |id| if (!endpoints.contains(id)) try endpoints.put(mutation.a, id, try mutation.ownPin(patch.next.key_template, id));
            if (edit.value) |value| {
                const pair = try parse(c.Pair, mutation.a, try encode(mutation.a, value));
                for ([_][]const u8{ pair.from, pair.to }) |id| if (!endpoints.contains(id)) try endpoints.put(mutation.a, id, try mutation.ownPin(patch.next.key_template, id));
            }
        };
        // Capture all old subtree weights before updating any ancestor.
        var deltas: std.ArrayList(PinDelta) = .empty;
        for (patch.edits) |edit| if (edit.kind == .redirect) try detached.put(mutation.a, edit.id, {});
        for (patch.edits) |edit| if (edit.kind == .redirect) {
            if (try mutation.get(resolver.KeyAlias, patch.next.key_template, .redirect, edit.id)) |edge| {
                const count = try mutation.pinCount(patch.next.key_template, edit.id);
                if (count != 0) {
                    try mutation.pinChild(patch.next.key_template, edge.target, edit.id, false);
                    try deltas.append(mutation.a, .{ .key = edge.target, .amount = -@as(i128, count) });
                }
            }
        };
        try mutation.propagatePins(patch.next.key_template, deltas.items, detached);
        var h = patch.next;
        h.bytes = if (previous) |old| old.bytes else 0;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (patch.edits) |edit| {
            const path = try mutation.record(h.key_template, edit.kind, edit.id);
            if ((try seen.getOrPut(mutation.a, path)).found_existing) return error.InvalidResolverCurationRequest;
            if (try mutation.backend.get(mutation.backend.ptr, mutation.a, path)) |old| {
                h.bytes = try std.math.sub(u64, h.bytes, old.len);
                try mutation.indexRecord(h.key_template, edit.kind, edit.id, old, false);
            }
            if (edit.value) |value_json| {
                const raw = try encode(mutation.a, value_json);
                h.bytes = try std.math.add(u64, h.bytes, raw.len);
                if (mutation.max_namespace_bytes) |maximum| if (h.bytes > maximum) return error.ResolverCurationLimitExceeded;
                try mutation.backend.put(mutation.backend.ptr, mutation.a, path, raw);
                try mutation.indexRecord(h.key_template, edit.kind, edit.id, raw, true);
            } else try mutation.backend.remove(mutation.backend.ptr, mutation.a, path);
        }
        var pin_ready = try parse(PinReady, mutation.a, (try mutation.backend.get(mutation.backend.ptr, mutation.a, try mutation.pinReadyPath(h.key_template))).?);
        deltas.clearRetainingCapacity();
        var changed_endpoints = endpoints.iterator();
        while (changed_endpoints.next()) |entry| {
            const now = try mutation.ownPin(h.key_template, entry.key_ptr.*);
            if (now != entry.value_ptr.*) {
                try deltas.append(mutation.a, .{ .key = entry.key_ptr.*, .amount = if (now) 1 else -1 });
                pin_ready.endpoints = if (now) try std.math.add(u64, pin_ready.endpoints, 1) else try std.math.sub(u64, pin_ready.endpoints, 1);
            }
        }
        try mutation.propagatePins(h.key_template, deltas.items, detached);
        if (endpoints.count() != 0) try mutation.backend.put(mutation.backend.ptr, mutation.a, try mutation.pinReadyPath(h.key_template), try encode(mutation.a, pin_ready));
        deltas.clearRetainingCapacity();
        for (patch.edits) |edit| if (edit.kind == .redirect) {
            if (try mutation.get(resolver.KeyAlias, h.key_template, .redirect, edit.id)) |edge| {
                const count = try mutation.pinCount(h.key_template, edit.id);
                if (count != 0) try deltas.append(mutation.a, .{ .key = edge.target, .amount = @as(i128, count) });
            }
        };
        // The final forest is now complete. Contributions from an attached
        // descendant flow through attached ancestors in one topological pass.
        try mutation.propagatePins(h.key_template, deltas.items, .empty);
        for (patch.edits) |edit| if (edit.kind == .redirect) {
            if (try mutation.get(resolver.KeyAlias, h.key_template, .redirect, edit.id)) |edge| {
                if (try mutation.pinCount(h.key_template, edit.id) != 0) try mutation.pinChild(h.key_template, edge.target, edit.id, true);
            }
        };
        @import("antfly_hash").Sha256.hash(try encode(mutation.a, .{ if (previous) |old| old.fingerprint else @as([32]u8, @splat(0)), h.key_template, h.revision, h.min_text_chars, h.adoption_revision, h.adoption_standalone, patch.edits }), &h.fingerprint, .{});
        if (enrolled and graph_edits.items.len != 0) {
            var routing: std.ArrayList([]const u8) = .empty;
            for (routing_edits.items) |edit| try routing.append(mutation.a, edit.id);
            var boundaries: std.ArrayList([]const u8) = .empty;
            var it = changes.keyIterator();
            while (it.next()) |id| try boundaries.append(mutation.a, id.*);
            std.mem.sort([]const u8, boundaries.items, {}, struct {
                fn less(_: void, left: []const u8, right: []const u8) bool {
                    return std.mem.lessThan(u8, left, right);
                }
            }.less);
            try mutation.appendPinEdit(h.key_template, (if (previous) |old| try mutation.queryGraph(old) else graph).fingerprint, graph.fingerprint, boundaries.items, routing.items, routing_targets.items);
        }
        try mutation.putQueryGraph(h, graph.fingerprint, graph.bytes);
        try mutation.putRouting(h, routing_fingerprint);
        if (previous == null) try mutation.indexHead(h);
        try mutation.backend.put(mutation.backend.ptr, mutation.a, try mutation.headPath(h.key_template), try encode(mutation.a, h));
    }
};

const Flat = struct { kind: Kind, id: []const u8, value: []const u8 };
fn flatten(a: A, namespace: policy.Namespace) ![]const Flat {
    var result: std.ArrayList(Flat) = .empty;
    for (namespace.merges) |v| try result.append(a, .{ .kind = .redirect, .id = v.key, .value = try encode(a, v) });
    for (namespace.state.suppress.keys) |v| try result.append(a, .{ .kind = .suppressed, .id = v, .value = try encode(a, v) });
    for (namespace.state.suppress.entries) |v| try result.append(a, .{ .kind = .suppression, .id = v.key, .value = try encode(a, v) });
    for (namespace.state.redirect_origins) |v| try result.append(a, .{ .kind = .redirect_origin, .id = v.key, .value = try encode(a, v) });
    for (namespace.state.removed_aliases) |v| try result.append(a, .{ .kind = .removed_alias, .id = v, .value = try encode(a, v) });
    for (namespace.state.distinct) |v| try result.append(a, .{ .kind = .distinct, .id = try encode(a, .{ v.from, v.to }), .value = try encode(a, v) });
    for (namespace.state.suppress.texts) |v| try result.append(a, .{ .kind = .text, .id = v, .value = try encode(a, v) });
    for (namespace.state.suppress.text_entries) |v| try result.append(a, .{ .kind = .text_origin, .id = v.key, .value = try encode(a, v) });
    return result.items;
}
/// Diff only the captured component, not the namespace's complete policy.
/// Union scopes from one immutable read cut without duplicating policy records.
pub fn unionNamespaces(a: A, left: policy.Namespace, right: policy.Namespace) !policy.Namespace {
    var records: std.StringHashMapUnmanaged(Flat) = .empty;
    for ([_]policy.Namespace{ left, right }) |namespace| for (try flatten(a, namespace)) |row| {
        const id = try std.fmt.allocPrint(a, "{s}/{s}", .{ @tagName(row.kind), row.id });
        const entry = try records.getOrPut(a, id);
        if (entry.found_existing and !std.mem.eql(u8, entry.value_ptr.value, row.value)) return error.InvalidCatalogRecord;
        entry.value_ptr.* = row;
    };
    var values: Store.Values = .{};
    var it = records.valueIterator();
    while (it.next()) |row| try values.append(a, row.kind, row.value);
    return values.snapshot(.{ .key_template = right.key_template, .revision = right.revision, .min_text_chars = right.state.suppress.min_text_chars }).namespace;
}

/// Streaming union of bounded query pages. Only unique facts survive a page;
/// transport bytes and duplicate paths can be released before the next read.
pub const QueryPolicyUnion = struct {
    suppression_projected: ?bool = null,
    configured_keys: std.StringArrayHashMapUnmanaged(void) = .empty,
    configured_texts: std.StringArrayHashMapUnmanaged(void) = .empty,
    configured_min_text_chars: u32 = 0,
    complete_aliases: ?bool = null,
    head: ?Head = null,
    seen: std.StringHashMapUnmanaged([]const u8) = .empty,
    values: Store.Values = .{},
    pub fn add(self: *QueryPolicyUnion, a: A, scratch: A, part: Snapshot) !void {
        const projected = part.configured_suppression != null;
        if (self.suppression_projected) |prior| {
            if (prior != projected) return error.ResolverCurationConflict;
        } else self.suppression_projected = projected;
        if (part.configured_suppression) |suppression| {
            self.configured_min_text_chars = @max(self.configured_min_text_chars, suppression.min_text_chars);
            for (suppression.keys) |key| if (!self.configured_keys.contains(key)) try self.configured_keys.put(a, try a.dupe(u8, key), {});
            for (suppression.texts) |text| if (!self.configured_texts.contains(text)) try self.configured_texts.put(a, try a.dupe(u8, text), {});
        }
        if (self.complete_aliases) |complete| {
            if (complete != part.complete_aliases) return error.ResolverCurationConflict;
        } else self.complete_aliases = part.complete_aliases;
        if (self.head) |h| {
            if (!std.mem.eql(u8, part.head.key_template, h.key_template) or part.head.revision != h.revision or !std.mem.eql(u8, &part.head.fingerprint, &h.fingerprint)) return error.ResolverCurationConflict;
        } else {
            self.head = part.head;
            self.head.?.key_template = try a.dupe(u8, part.head.key_template);
        }
        for (try flatten(scratch, part.namespace)) |record_value| {
            const key = try std.fmt.allocPrint(scratch, "{s}\x00{s}", .{ @tagName(record_value.kind), record_value.id });
            if (self.seen.get(key)) |prior| {
                if (!std.mem.eql(u8, prior, record_value.value)) return error.ResolverCurationConflict;
                continue;
            }
            try self.seen.put(a, try a.dupe(u8, key), try a.dupe(u8, record_value.value));
            try self.values.append(a, record_value.kind, record_value.value);
        }
    }
    pub fn finish(self: QueryPolicyUnion) !Snapshot {
        var result = self.values.snapshot(self.head orelse return error.InvalidResolverCurationRequest);
        result.complete_aliases = self.complete_aliases orelse false;
        if (self.suppression_projected orelse false) result.configured_suppression = .{ .keys = self.configured_keys.keys(), .texts = self.configured_texts.keys(), .min_text_chars = self.configured_min_text_chars };
        return result;
    }
};
pub fn unionQueryPolicies(a: A, parts: []const Snapshot) !Snapshot {
    var joined: QueryPolicyUnion = .{};
    for (parts) |part| try joined.add(a, a, part);
    return joined.finish();
}

pub fn difference(a: A, before: policy.Namespace, after: policy.Namespace) !Patch {
    const old = try flatten(a, before);
    const next = try flatten(a, after);
    var by_key: std.StringHashMapUnmanaged(Flat) = .empty;
    for (old) |v| try by_key.put(a, try std.fmt.allocPrint(a, "{s}/{s}", .{ @tagName(v.kind), v.id }), v);
    var edits: std.ArrayList(Edit) = .empty;
    for (next) |v| {
        const key = try std.fmt.allocPrint(a, "{s}/{s}", .{ @tagName(v.kind), v.id });
        const previous = by_key.fetchRemove(key);
        if (previous == null or !std.mem.eql(u8, previous.?.value.value, v.value)) try edits.append(a, .{ .kind = v.kind, .id = v.id, .value = try parse(std.json.Value, a, v.value) });
    }
    var removed = by_key.valueIterator();
    while (removed.next()) |v| try edits.append(a, .{ .kind = v.kind, .id = v.id, .value = null });
    return .{ .expected_revision = before.revision, .next = .{ .key_template = after.key_template, .revision = after.revision, .adoption_revision = after.adoption_revision, .adoption_standalone = after.adoption_standalone, .min_text_chars = after.state.suppress.min_text_chars }, .edits = edits.items };
}

test "native resolver authority seeks a component and patches only changed records" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Memory = TestMemory;
    var memory: Memory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = Memory.get, .put = Memory.put, .remove = Memory.remove, .scan = Memory.scan } };
    var aliases: [10000]resolver.KeyAlias = undefined;
    for (&aliases, 0..) |*v, i| v.* = .{ .key = try std.fmt.allocPrint(a, "entity/a{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/b{d}", .{i}) };
    const before: policy.Namespace = .{ .key_template = "entity/{{_key}}" };
    const imported: policy.Namespace = .{ .key_template = before.key_template, .revision = 1, .merges = &aliases };
    try store.apply(try difference(a, before, imported));
    memory.reads = 0;
    memory.writes = 0;
    memory.visited = 0;
    const component = (try store.component(before.key_template, &.{"entity/a42"})).?;
    try std.testing.expectEqual(@as(usize, 1), component.namespace.merges.len);
    try std.testing.expect(memory.reads < 32 and memory.visited < 8);
    var next = component.namespace;
    next.revision = 2;
    next.merges = &.{};
    next.state.removed_aliases = &.{"entity/a42"};
    const patch = try difference(a, component.namespace, next);
    try std.testing.expectEqual(@as(usize, 2), patch.edits.len);
    try store.apply(patch);
    // A namespace without a qualified static owner enrolls no unused log.
    try std.testing.expect(memory.writes < 8);
    try std.testing.expectError(error.ResolverCurationConflict, store.apply(patch));
    const unrelated = (try store.component(before.key_template, &.{"entity/a43"})).?;
    try std.testing.expectEqualStrings("entity/b43", unrelated.namespace.merges[0].target);
    const bridged = (try store.componentWithAliases(before.key_template, &.{"entity/static"}, &.{.{ .key = "entity/static", .target = "entity/a43" }})).?;
    try std.testing.expectEqual(@as(usize, 1), bridged.namespace.merges.len);
    try std.testing.expectEqualStrings("entity/b43", bridged.namespace.merges[0].target);
    try store.adoptNamespace(.{ .key_template = "legacy/{{_key}}", .merges = &.{.{ .key = "legacy/a", .target = "legacy/b" }} });
    try std.testing.expectEqual(@as(u64, 0), (try store.head("legacy/{{_key}}")).?.revision);
    const proposed = try c.propose(a, .{}, &.{}, .{ .action = .merge, .key = "entity/a1", .into = "entity/b1" }, .{});
    try store.applyQueue(.{ .expected_revision = 0, .revision = 1, .proposals = proposed.state.proposals });
    const queued = try store.proposalPage(.open, .merge, null, 1);
    try std.testing.expectEqual(@as(usize, 1), queued.proposals.len);
    try std.testing.expect(!try store.queueOnly(queued.proposals[0].id));
    try std.testing.expectError(error.ResolverCurationConflict, store.markQueueOnly(queued.proposals[0].id));
    const reservation = (try store.queueHead()).bytes;
    try std.testing.expect(reservation > c.decision_origin_byte_budget);
    const decision = try c.decide(a, proposed.state, &.{}, &.{}, queued.proposals[0].id, .{ .accept = false }, .{});
    try store.applyQueue(.{ .expected_revision = 1, .revision = 2, .proposals = decision.state.proposals });
    try store.markQueueOnly(queued.proposals[0].id);
    try std.testing.expect(try store.queueOnly(queued.proposals[0].id));
    try std.testing.expect((try store.queueHead()).bytes < reservation);
    try std.testing.expectEqual(@as(usize, 0), (try store.proposalPage(.open, null, null, 1)).proposals.len);
    try std.testing.expectEqual(@as(usize, 1), (try store.proposalPage(.rejected, .merge, null, 1)).proposals.len);
    try std.testing.expectEqual(@as(usize, 0), (try store.proposalsForKeys(&.{"entity/a1"})).proposals.len);
    try std.testing.expectError(error.ResolverCurationConflict, store.applyQueue(.{ .expected_revision = 1, .revision = 3 }));
    var portable = memory.rows.iterator();
    while (portable.next()) |row| {
        const key = try std.mem.concat(a, u8, &.{ lite_prefix, row.key_ptr.*[store.prefix.len..] });
        try validatePortableRow(a, key, row.value_ptr.*);
    }
    try validatePortableRow(a, lite_marker, "1");
    try std.testing.expectError(error.InvalidMetadataBatch, validatePortableRow(a, lite_marker, "2"));
}

pub fn Transaction(comptime Txn: type) type {
    return struct {
        const Tree = std.Treap([]const u8, struct {
            fn order(l: []const u8, r: []const u8) std.math.Order {
                return std.mem.order(u8, l, r);
            }
        }.order);
        txn: *Txn,
        staged: std.StringHashMapUnmanaged(?[]const u8) = .empty,
        ordered: Tree = .{},
        scan_steps: usize = 0,
        fn stage(self: *@This(), a: A, key: []const u8, raw: ?[]const u8) !void {
            const slot = try self.staged.getOrPut(a, key);
            slot.value_ptr.* = raw;
            if (!slot.found_existing) {
                var entry = self.ordered.getEntryFor(slot.key_ptr.*);
                entry.set(try a.create(Tree.Node));
            }
        }
        fn get(ptr: *anyopaque, a: std.mem.Allocator, key: []const u8) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.staged.getEntry(key)) |entry| return entry.value_ptr.*;
            const raw = self.txn.get(key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            return try a.dupe(u8, raw);
        }
        fn put(ptr: *anyopaque, a: std.mem.Allocator, key: []const u8, raw: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.stage(a, key, raw);
        }
        fn remove(ptr: *anyopaque, a: std.mem.Allocator, key: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.stage(a, key, null);
        }
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, prefix: []const u8, after: ?[]const u8, limit: u32) !Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var cursor = try self.txn.openCursor();
            defer cursor.close();
            var row = try cursor.seekAtOrAfter(after orelse prefix);
            if (after) |last| if (row) |entry| if (std.mem.eql(u8, entry.key, last)) {
                row = try cursor.next();
            };
            if (limit == 0) return error.InvalidResolverCurationRequest;
            const lower = after orelse prefix;
            var search = self.ordered.root;
            var pending: ?*Tree.Node = null;
            while (search) |node| {
                if (@import("builtin").is_test) self.scan_steps += 1;
                if (std.mem.order(u8, node.key, lower) == .lt or (after != null and std.mem.eql(u8, node.key, lower))) search = node.children[1] else {
                    pending = node;
                    search = node.children[0];
                }
            }
            var rows: std.ArrayList(Row) = .empty;
            while (rows.items.len <= limit) {
                if (row) |entry| if (!std.mem.startsWith(u8, entry.key, prefix)) {
                    row = null;
                };
                if (pending) |node| if (!std.mem.startsWith(u8, node.key, prefix)) {
                    pending = null;
                };
                if (row == null and pending == null) break;
                const order = if (row == null) std.math.Order.lt else if (pending == null) std.math.Order.gt else std.mem.order(u8, pending.?.key, row.?.key);
                if (order != .gt) {
                    const node = pending.?;
                    if (@import("builtin").is_test) self.scan_steps += 1;
                    if (self.staged.get(node.key).?) |raw| try rows.append(a, .{ .key = node.key, .value = raw });
                    pending = node.next();
                    if (order == .eq) row = try cursor.next();
                } else {
                    const entry = row.?;
                    try rows.append(a, .{ .key = try a.dupe(u8, entry.key), .value = try a.dupe(u8, entry.value) });
                    row = try cursor.next();
                }
            }
            const count = @min(limit, rows.items.len);
            return .{ .rows = rows.items[0..count], .next = if (rows.items.len > count) rows.items[count - 1].key else null };
        }

        pub fn flush(self: *@This(), a: std.mem.Allocator) !void {
            var keys: std.ArrayList([]const u8) = .empty;
            var it = self.staged.keyIterator();
            while (it.next()) |key| try keys.append(a, key.*);
            std.mem.sort([]const u8, keys.items, {}, struct {
                fn less(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.lessThan(u8, l, r);
                }
            }.less);
            for (keys.items) |key| if (self.staged.get(key).?) |raw| try self.txn.put(key, raw) else self.txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.staged.clearRetainingCapacity();
            self.ordered = .{};
        }
        pub fn backendGet(self: *@This(), a: A, key: []const u8) !?[]const u8 {
            return get(self, a, key);
        }
        pub fn backendPut(self: *@This(), a: A, key: []const u8, raw: []const u8) !void {
            return put(self, a, key, raw);
        }
        pub fn store(self: *@This(), alloc: A, prefix: []const u8) Store {
            return .{ .a = alloc, .prefix = prefix, .backend = .{ .ptr = self, .get = get, .put = put, .remove = remove, .scan = scan } };
        }
    };
}

/// Validate the typed portable envelope without allocating a runtime catalog.
/// Native snapshot/portable checksums additionally bind all record/index bytes.
pub fn validatePortableRow(a: A, key: []const u8, raw: []const u8) !void {
    if (!std.mem.startsWith(u8, key, lite_prefix) or raw.len > c.max_state_bytes) return error.InvalidMetadataBatch;
    const tail = key[lite_prefix.len..];
    if (std.mem.eql(u8, tail, "version")) {
        if (!std.mem.eql(u8, raw, "1")) return error.InvalidMetadataBatch;
        return;
    }
    if (std.mem.eql(u8, tail, "head-routes-ready")) {
        if (!std.mem.eql(u8, raw, "1")) return error.InvalidMetadataBatch;
        return;
    }
    var parts: [6][]const u8 = undefined;
    var count: usize = 0;
    var it = std.mem.splitScalar(u8, tail, '/');
    while (it.next()) |part| {
        if (count == parts.len or part.len == 0) return error.InvalidMetadataBatch;
        parts[count] = part;
        count += 1;
    }
    if ((count == 2 and std.mem.eql(u8, parts[0], "pin-ready")) or (count == 3 and std.mem.eql(u8, parts[0], "pin-count"))) {
        for (parts[1..count]) |part| {
            if (part.len != 64) return error.InvalidMetadataBatch;
            for (part) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        }
        if (count == 2) {
            _ = try parse(Store.PinReady, a, raw);
        } else {
            const value = try parse(Store.PinCount, a, raw);
            try c.validKey(value.key);
            if (value.count == 0 or !std.mem.eql(u8, parts[2], try hash(a, value.key))) return error.InvalidMetadataBatch;
        }
        return;
    }
    if (count == 2 and std.mem.eql(u8, parts[0], "head-route-node")) {
        if (parts[1].len != 64 or (!std.mem.eql(u8, raw, "0") and !std.mem.eql(u8, raw, "1"))) return error.InvalidMetadataBatch;
        for (parts[1]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        return;
    }
    if (count == 3 and std.mem.eql(u8, parts[0], "head-route")) {
        const template = try parse([]const u8, a, raw);
        if (!std.mem.eql(u8, parts[1], try hash(a, Store.headRoutePrefix(template))) or !std.mem.eql(u8, parts[2], try hash(a, template))) return error.InvalidMetadataBatch;
        return;
    }
    if (count == 2 and std.mem.eql(u8, parts[0], "head")) {
        const h = try parse(Head, a, raw);
        if (h.version != 1 or h.key_template.len == 0 or h.bytes > c.max_state_bytes or !std.mem.eql(u8, parts[1], try hash(a, h.key_template))) return error.InvalidMetadataBatch;
        return;
    }
    if (count == 4 and std.mem.eql(u8, parts[0], "record")) {
        const kind = std.meta.stringToEnum(Kind, parts[2]) orelse return error.InvalidMetadataBatch;
        var values: Store.Values = .{};
        try values.append(a, kind, raw);
        const namespace = values.snapshot(.{ .key_template = "", .revision = 0 }).namespace;
        const entries = try flatten(a, namespace);
        if (entries.len != 1 or !std.mem.eql(u8, parts[3], try hash(a, entries[0].id))) return error.InvalidMetadataBatch;
        _ = try c.parseState(a, try encode(a, namespace.state));
        _ = try c.RedirectIndex.init(a, namespace.merges);
        for (namespace.merges) |alias| {
            try c.validKey(alias.key);
            try c.validKey(alias.target);
        }
        if (parts[1].len != 64) return error.InvalidMetadataBatch;
        for (parts[1]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        return;
    }
    if (std.mem.eql(u8, parts[0], "pin-edits-v1")) {
        if (raw.len > 64 * 1024) return error.InvalidMetadataBatch;
        if (count != 3 and count != 4 and count != 5) return error.InvalidMetadataBatch;
        if (parts[1].len != 64) return error.InvalidMetadataBatch;
        for (parts[1]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        if (count == 3 and std.mem.eql(u8, parts[2], "head")) {
            const head = try parse(Store.PinEditHead, a, raw);
            if (head.barrier > head.sequence) return error.InvalidMetadataBatch;
            return;
        }
        if (count == 4 and (std.mem.eql(u8, parts[2], "compact") or std.mem.eql(u8, parts[2], "routed-compact"))) {
            const slot = std.fmt.parseInt(u16, parts[3], 10) catch return error.InvalidMetadataBatch;
            if (parts[3].len != 3 or slot >= 256 or raw.len < 8 or (raw.len - 8) % 32 != 0 or raw.len > 8 + 640 * 32) return error.InvalidMetadataBatch;
            if (std.mem.eql(u8, parts[2], "routed-compact") and raw.len > 8 + 128 * 32) return error.InvalidMetadataBatch;
            const sequence = std.mem.readInt(u64, raw[0..8], .little);
            if (sequence == 0 or sequence % 256 != slot) return error.InvalidMetadataBatch;
            return;
        }
        if (count == 4 and (std.mem.eql(u8, parts[2], "invalid-key") or std.mem.eql(u8, parts[2], "tail-key") or std.mem.eql(u8, parts[2], "routed-key"))) {
            if (parts[3].len != 64) return error.InvalidMetadataBatch;
            for (parts[3]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
            if (try parse(u64, a, raw) == 0) return error.InvalidMetadataBatch;
            return;
        }
        if (count == 5 and (std.mem.eql(u8, parts[2], "invalidated") or std.mem.eql(u8, parts[2], "tails") or std.mem.eql(u8, parts[2], "routed"))) {
            const value = try parse(Store.PinInvalidation, a, raw);
            try c.validKey(value.key);
            if (parts[3].len != 20 or value.sequence == 0 or (std.fmt.parseInt(u64, parts[3], 10) catch return error.InvalidMetadataBatch) != value.sequence or !std.mem.eql(u8, parts[4], try hash(a, value.key))) return error.InvalidMetadataBatch;
            return;
        }
        if (count != 4 or !std.mem.eql(u8, parts[2], "slot") or parts[3].len != 3) return error.InvalidMetadataBatch;
        const slot = std.fmt.parseInt(u16, parts[3], 10) catch return error.InvalidMetadataBatch;
        const event = try parse(Store.PinEdit, a, raw);
        if (slot >= 256 or event.sequence == 0 or event.sequence % 256 != slot or event.boundaries.len > 256 or event.routing.len > 128 or event.targets.len > 256 or event.chained_targets.len > 256 or event.invalidations.len > 640) return error.InvalidMetadataBatch;
        for (event.boundaries) |id| try c.validKey(id);
        for (event.routing) |id| try c.validKey(id);
        for (event.targets) |id| try c.validKey(id);
        for (event.chained_targets) |id| {
            try c.validKey(id);
            var found = false;
            for (event.targets) |target| if (std.mem.eql(u8, target, id)) {
                found = true;
                break;
            };
            if (!found) return error.InvalidMetadataBatch;
        }
        for (event.invalidations) |id| try c.validKey(id);
        return;
    }
    if (count == 2 and std.mem.eql(u8, parts[0], "query-pins-gc-v1")) {
        if (parts[1].len != 64) return error.InvalidMetadataBatch;
        for (parts[1]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        const state = try parse(Store.PinProjectionGC, a, raw);
        if (state.complete and state.after != null) return error.InvalidMetadataBatch;
        if (state.after) |after| {
            if (after.len != 64 and !(after.len == 65 and after[64] == '~')) return error.InvalidMetadataBatch;
            for (after[0..64]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        }
        return;
    }
    if (count == 1 and (std.mem.eql(u8, parts[0], "query-pins-owner-enrolled-v1") or std.mem.eql(u8, parts[0], "query-pins-owner-enrolled-v2") or std.mem.eql(u8, parts[0], "query-pins-owner-enrolled-v3") or std.mem.eql(u8, parts[0], "query-pins-owner-enrolled-v4") or std.mem.eql(u8, parts[0], "query-pins-owner-enrolled-v5") or std.mem.eql(u8, parts[0], "query-pins-owner-enrolled-v6") or std.mem.eql(u8, parts[0], "query-pins-owner-enrolled-v7"))) {
        if (!std.mem.eql(u8, raw, "1")) return error.InvalidMetadataBatch;
        return;
    }
    if (std.mem.eql(u8, parts[0], "query-pins-v1")) {
        if (count != 4 and count != 5 and count != 6) return error.InvalidMetadataBatch;
        for (parts[1..3]) |digest| {
            if (digest.len != 64) return error.InvalidMetadataBatch;
            for (digest) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        }
        if (count == 4 and std.mem.eql(u8, parts[3], "state")) {
            const state = try parse(Store.PinProjection, a, raw);
            if (state.phase == .ready and (state.after != null or state.walk != null or state.repair != null or state.trace_clean)) return error.InvalidMetadataBatch;
            if (state.format != 0 and state.format != 2 and state.format != 3 and state.format != 4 and state.format != Store.pin_projection_format) return error.InvalidMetadataBatch;
            if (state.repair) |repair| {
                if ((state.phase != .repair and state.phase != .nodes and state.phase != .guards and state.phase != .build) or state.sequence == null or repair.sequence > repair.target +| 1 or repair.index > 640) return error.InvalidMetadataBatch;
                if (repair.root) |id| try c.validKey(id);
            } else if (state.phase == .repair) return error.InvalidMetadataBatch;
            if (state.walk) |walk| {
                if ((state.phase != .nodes and state.phase != .build and state.phase != .repair) or walk.hops > 16383) return error.InvalidMetadataBatch;
                try c.validKey(walk.key);
                try c.validKey(walk.current);
                if (walk.root) |terminal| try c.validKey(terminal);
            }
            if (state.after) |after| {
                if (after.len != 64) return error.InvalidMetadataBatch;
                for (after) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
            }
            return;
        }
        if (count == 5 and std.mem.eql(u8, parts[3], "dep-summary")) {
            if (parts[4].len != 64) return error.InvalidMetadataBatch;
            for (parts[4]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
            dependency_filter.validateRoot(raw) catch return error.InvalidMetadataBatch;
            return;
        }
        if (count == 6 and std.mem.eql(u8, parts[3], "dep-leaf")) {
            if (parts[4].len != 64 or parts[5].len != 16) return error.InvalidMetadataBatch;
            for (parts[4]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
            const upper = std.fmt.parseInt(u64, parts[5], 16) catch return error.InvalidMetadataBatch;
            dependency_filter.validateLeaf(raw, upper) catch return error.InvalidMetadataBatch;
            return;
        }
        if (count == 6 and (std.mem.eql(u8, parts[3], "dep-count") or std.mem.eql(u8, parts[3], "dep-overflow"))) {
            if (parts[4].len != 64) return error.InvalidMetadataBatch;
            for (parts[4]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
            const position = std.fmt.parseInt(usize, parts[5], 16) catch return error.InvalidMetadataBatch;
            if (std.mem.eql(u8, parts[3], "dep-count")) {
                if (parts[5].len != 2 or position >= 128 or raw.len != 1024 or std.mem.allEqual(u8, raw, 0)) return error.InvalidMetadataBatch;
            } else if (parts[5].len != 5 or position >= 262144 or try parse(u32, a, raw) == 0) return error.InvalidMetadataBatch;
            return;
        }
        if ((count == 5 and std.mem.eql(u8, parts[3], "guard")) or (count == 6 and std.mem.eql(u8, parts[3], "root-guard"))) {
            const guard = try parse(Store.PinGuard, a, raw);
            try c.validKey(guard.key);
            try c.validKey(guard.parent);
            try c.validKey(guard.root);
            if (!std.mem.eql(u8, parts[count - 1], try hash(a, guard.key))) return error.InvalidMetadataBatch;
            if (count == 6 and (!guard.latent or !std.mem.eql(u8, parts[4], try hash(a, guard.root)))) return error.InvalidMetadataBatch;
            return;
        }
        if (count == 6 and std.mem.eql(u8, parts[3], "guard-in")) {
            const edge = try parse(Store.GuardEdge, a, raw);
            try c.validKey(edge.key);
            try c.validKey(edge.parent);
            if (!std.mem.eql(u8, parts[4], try hash(a, edge.parent)) or !std.mem.eql(u8, parts[5], try hash(a, edge.key))) return error.InvalidMetadataBatch;
            return;
        }
        if (count == 5 and (std.mem.eql(u8, parts[3], "path") or std.mem.eql(u8, parts[3], "boundary"))) {
            const path = try parse(Store.PinPath, a, raw);
            try c.validKey(path.key);
            try c.validKey(path.root);
            if (path.next) |next| try c.validKey(next);
            if (!std.mem.eql(u8, parts[4], try hash(a, path.key))) return error.InvalidMetadataBatch;
            return;
        }
        if (count == 5 and std.mem.eql(u8, parts[3], "dirty-node")) {
            const node = try parse(Store.DirtyPinNode, a, raw);
            try c.validKey(node.key);
            if (node.root) |value| try c.validKey(value);
            if (!std.mem.eql(u8, parts[4], try hash(a, node.key))) return error.InvalidMetadataBatch;
            return;
        }
        if (count == 5 and (std.mem.eql(u8, parts[3], "dirty-boundary") or std.mem.eql(u8, parts[3], "dirty-root") or std.mem.eql(u8, parts[3], "repair-root") or std.mem.eql(u8, parts[3], "pending-node") or std.mem.eql(u8, parts[3], "prune-node") or std.mem.eql(u8, parts[3], "next-node") or std.mem.eql(u8, parts[3], "next-boundary") or std.mem.eql(u8, parts[3], "dirty-guard") or std.mem.eql(u8, parts[3], "trace"))) {
            const id = try parse([]const u8, a, raw);
            try c.validKey(id);
            if (!std.mem.eql(u8, parts[4], try hash(a, id))) return error.InvalidMetadataBatch;
            return;
        }
        if (count != 6 or (!std.mem.eql(u8, parts[3], "member") and !std.mem.eql(u8, parts[3], "root-path") and !std.mem.eql(u8, parts[3], "path-in"))) return error.InvalidMetadataBatch;
        const member = try parse(Store.PinPath, a, raw);
        if (member.next) |next| try c.validKey(next);
        try c.validKey(member.root);
        try c.validKey(member.key);
        if (!std.mem.eql(u8, parts[4], try hash(a, if (std.mem.eql(u8, parts[3], "path-in")) member.next orelse return error.InvalidMetadataBatch else member.root)) or !std.mem.eql(u8, parts[5], try hash(a, member.key))) return error.InvalidMetadataBatch;
        return;
    }
    if (count == 2 and std.mem.eql(u8, parts[0], "queue") and std.mem.eql(u8, parts[1], "head")) {
        const h = try parse(QueueHead, a, raw);
        if (h.version != 1 or h.bytes > c.max_state_bytes or h.count > 4096) return error.InvalidMetadataBatch;
        _ = try c.parseState(a, try encode(a, c.State{ .receipt_scope = h.receipt_scope }));
        return;
    }
    if (count == 3 and std.mem.eql(u8, parts[0], "queue") and std.mem.eql(u8, parts[1], "proposal")) {
        const proposal = try parse(c.Proposal, a, raw);
        _ = try c.parseState(a, try encode(a, c.State{ .proposals = &.{proposal} }));
        if (!std.mem.eql(u8, parts[2], proposal.id)) return error.InvalidMetadataBatch;
        return;
    }
    if (count == 3 and std.mem.eql(u8, parts[0], "queue") and std.mem.eql(u8, parts[1], "header")) {
        const header = try parse(ProposalHeader, a, raw);
        if (header.version != 1 or header.proposal.evidence != null or !std.mem.eql(u8, parts[2], header.proposal.id)) return error.InvalidMetadataBatch;
        _ = try c.parseState(a, try encode(a, c.State{ .proposals = &.{header.proposal} }));
        return;
    }
    if (count == 2 and std.mem.eql(u8, tail, "queue/ownership-progress")) {
        const progress = try parse(Store.OwnershipProgress, a, raw);
        if (progress.complete and progress.after != null) return error.InvalidMetadataBatch;
        if (progress.after) |after| {
            if (after.len != 64) return error.InvalidMetadataBatch;
            for (after) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        }
        return;
    }
    if (count >= 4 and std.mem.eql(u8, parts[0], "queue") and std.mem.eql(u8, parts[1], "ownership")) {
        if (count == 4 and std.mem.eql(u8, parts[2], "member")) {
            const owned_key = try parse([]const u8, a, raw);
            try c.validKey(owned_key);
            if (!std.mem.eql(u8, parts[3], try hash(a, owned_key))) return error.InvalidMetadataBatch;
            return;
        }
        if (!std.mem.eql(u8, parts[2], "forward") and !std.mem.eql(u8, parts[2], "reverse")) return error.InvalidMetadataBatch;
        if (count != 5 and count != 6) return error.InvalidMetadataBatch;
        if (parts[4].len != 64) return error.InvalidMetadataBatch;
        for (parts[4]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        if (count == 5 and std.mem.eql(u8, parts[3], "node")) {
            const node = try parse(Store.Ownership.Node, a, raw);
            if (node.refs == 0) return error.InvalidMetadataBatch;
            if (node.terminal) |owned_key| {
                try c.validKey(owned_key);
                const indexed = try Store.Ownership.encodedKey(a, owned_key, std.mem.eql(u8, parts[2], "reverse"));
                if (!std.mem.eql(u8, parts[4], try hash(a, indexed))) return error.InvalidMetadataBatch;
            }
            return;
        }
        if (count == 6 and std.mem.eql(u8, parts[3], "edge")) {
            const edge = try parse(Store.Ownership.Edge, a, raw);
            if (edge.label.len == 0 or edge.child.len != 64 or parts[5].len != 2) return error.InvalidMetadataBatch;
            for (edge.label) |byte| if (!std.ascii.isHex(byte) or (byte >= 'A' and byte <= 'F')) return error.InvalidMetadataBatch;
            if (!std.mem.eql(u8, parts[5], try std.fmt.allocPrint(a, "{x:0>2}", .{edge.label[0]}))) return error.InvalidMetadataBatch;
            for (edge.child) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
            return;
        }
        return error.InvalidMetadataBatch;
    }
    if (count == 2 and std.mem.eql(u8, parts[0], "query-routing")) {
        if (parts[1].len != 64) return error.InvalidMetadataBatch;
        for (parts[1]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        _ = try parse(Store.RoutingStamp, a, raw);
        return;
    }
    if (count == 2 and std.mem.eql(u8, parts[0], "query-graph")) {
        if (parts[1].len != 64) return error.InvalidMetadataBatch;
        for (parts[1]) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        _ = try parse(Store.QueryGraph, a, raw);
        return;
    }
    // Index envelopes contain a typed key/ID or an incident distinct pair.
    if (count == 4 and (std.mem.eql(u8, parts[0], "incoming") or std.mem.eql(u8, parts[0], "cascade") or std.mem.eql(u8, parts[0], "pin-child") or std.mem.eql(u8, parts[0], "distinct-key"))) {
        for (parts[1..4]) |part| {
            if (part.len != 64) return error.InvalidMetadataBatch;
            for (part) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        }
        if (std.mem.eql(u8, parts[0], "distinct-key")) {
            _ = try parse(c.Pair, a, raw);
        } else try c.validKey(try parse([]const u8, a, raw));
        return;
    }
    if (count >= 3 and std.mem.eql(u8, parts[0], "queue")) {
        if (std.mem.eql(u8, parts[1], "owned")) {
            const id = try parse([]const u8, a, raw);
            try c.validKey(id);
            if (count != 3 or !std.mem.eql(u8, parts[2], try hash(a, id))) return error.InvalidMetadataBatch;
            return;
        }
        const id = try parse([]const u8, a, raw);
        if (id.len != 64 or !std.mem.eql(u8, parts[count - 1], id)) return error.InvalidMetadataBatch;
        for (id) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMetadataBatch;
        if (std.mem.eql(u8, parts[1], "queue-only") and count == 3) return;
        if (std.mem.eql(u8, parts[1], "state") and count == 4 and std.meta.stringToEnum(c.ProposalState, parts[2]) != null) return;
        if (std.mem.eql(u8, parts[1], "action") and count == 4 and std.meta.stringToEnum(c.EntityAction, parts[2]) != null) return;
        if (std.mem.eql(u8, parts[1], "filter") and count == 5 and std.meta.stringToEnum(c.ProposalState, parts[2]) != null and std.meta.stringToEnum(c.EntityAction, parts[3]) != null) return;
        if (std.mem.eql(u8, parts[1], "key") and count == 4 and parts[2].len == 64) return;
    }
    return error.InvalidMetadataBatch;
}

const TestMemory = struct {
    a: A,
    rows: std.StringHashMapUnmanaged([]const u8) = .empty,
    reads: usize = 0,
    read_bytes: usize = 0,
    write_bytes: usize = 0,
    writes: usize = 0,
    visited: usize = 0,
    distinct_rows: usize = 0,
    scans: usize = 0,
    fn get(ptr: *anyopaque, _: A, key: []const u8) !?[]const u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.reads += 1;
        const raw = self.rows.get(key);
        if (raw) |value| self.read_bytes += value.len;
        return raw;
    }
    fn put(ptr: *anyopaque, _: A, key: []const u8, raw: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        self.write_bytes += raw.len;
        try self.rows.put(self.a, key, raw);
    }
    fn remove(ptr: *anyopaque, _: A, key: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        _ = self.rows.remove(key);
    }
    fn scan(ptr: *anyopaque, alloc: A, prefix: []const u8, after: ?[]const u8, limit: u32) !Page {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.scans += 1;
        var rows: std.ArrayList(Row) = .empty;
        // Match storage's bounded page allocation, including one lookahead.
        // A fixture must not materialize the entire suffix for a one-row seek.
        var it = self.rows.iterator();
        while (it.next()) |v| if (std.mem.startsWith(u8, v.key_ptr.*, prefix) and (after == null or std.mem.order(u8, v.key_ptr.*, after.?) == .gt)) {
            var at: usize = 0;
            while (at < rows.items.len and std.mem.lessThan(u8, rows.items[at].key, v.key_ptr.*)) at += 1;
            if (at > limit) continue;
            if (rows.items.len > limit) _ = rows.pop();
            try rows.insert(alloc, at, .{ .key = v.key_ptr.*, .value = v.value_ptr.* });
        };
        const count = @min(limit, rows.items.len);
        self.visited += count;
        if (std.mem.endsWith(u8, prefix, "/distinct/")) self.distinct_rows += count;
        return .{ .rows = rows.items[0..count], .next = if (count < rows.items.len) rows.items[count - 1].key else null };
    }
};

test "native resolver authority closes alternating static and native alias paths" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    try store.apply(try difference(a, .{ .key_template = template }, .{ .key_template = template, .revision = 1, .merges = &.{ .{ .key = "entity/c", .target = "entity/d" }, .{ .key = "entity/e", .target = "entity/a" } } }));
    const aliases = [_]resolver.KeyAlias{ .{ .key = "entity/b", .target = "entity/c" }, .{ .key = "entity/d", .target = "entity/e" } };
    const scoped = (try store.componentWithAliases(template, &.{ "entity/a", "entity/b" }, &aliases)).?;
    try std.testing.expectEqual(@as(usize, 2), scoped.namespace.merges.len);
    var effective: std.ArrayList(resolver.KeyAlias) = .empty;
    try effective.appendSlice(a, scoped.namespace.merges);
    try effective.appendSlice(a, &aliases);
    try effective.append(a, .{ .key = "entity/a", .target = "entity/b" });
    try std.testing.expectError(error.InvalidResolverAliasCycle, c.RedirectIndex.init(a, effective.items));
}

test "resolver curation shared planning sees every static profile independent of order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const namespace: policy.Namespace = .{ .key_template = "entity/{{_key}}" };
    const empty: AliasProfile = .{ .key_template = namespace.key_template, .aliases = &.{} };
    const declared: AliasProfile = .{ .key_template = namespace.key_template, .aliases = &.{.{ .key = "entity/a", .target = "entity/b" }} };
    for ([_][2]AliasProfile{ .{ empty, declared }, .{ declared, empty } }) |profiles| {
        const effective = try effectiveAliases(a, namespace, &profiles, "entity/a", null);
        const unmerge = try c.plan(a, "{}", namespace.merges, effective, .{ .action = .unmerge, .key = "entity/a" });
        try std.testing.expect(unmerge.changed);
        try std.testing.expectEqualStrings("entity/a", unmerge.state.removed_aliases[0]);
        const shared = try c.planWithOrigin(a, "{}", namespace.merges, try planningAliases(a, namespace, effective, .merge, "entity/a", "entity/b"), .{ .action = .merge, .key = "entity/a", .to = "entity/b" }, .{ .actor = "curator", .revision = 7 });
        try std.testing.expect(shared.changed);
        try std.testing.expectEqualStrings("curator", shared.state.redirect_origins[0].origin.actor);
        try validateProfiles(a, .{ .key_template = namespace.key_template, .state = unmerge.state }, &profiles);
    }
}

test "resolver curation transactional range scans ignore ten thousand unrelated staged writes" {
    const Cursor = struct {
        fn seekAtOrAfter(_: *@This(), _: []const u8) !?Row {
            return null;
        }
        fn next(_: *@This()) !?Row {
            return null;
        }
        fn close(_: *@This()) void {}
    };
    const Txn = struct {
        fn get(_: *@This(), _: []const u8) anyerror![]const u8 {
            return error.NotFound;
        }
        fn openCursor(_: *@This()) !Cursor {
            return .{};
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: Txn = .{};
    var overlay: Transaction(Txn) = .{ .txn = &txn };
    const store = overlay.store(a, "");
    for (0..10000) |i| try store.backend.put(&overlay, a, try std.fmt.allocPrint(a, "unrelated/{d:0>5}", .{i}), "false");
    try store.backend.put(&overlay, a, "narrow/a", "true");
    try store.backend.put(&overlay, a, "narrow/b", "true");
    const first = try store.backend.scan(&overlay, a, "narrow/", null, 1);
    try std.testing.expectEqualStrings("narrow/a", first.rows[0].key);
    try std.testing.expect(first.next != null);
    const second = try store.backend.scan(&overlay, a, "narrow/", first.next, 1);
    try std.testing.expectEqualStrings("narrow/b", second.rows[0].key);
    try std.testing.expect(second.next == null);
    try std.testing.expect(overlay.scan_steps < 128);
}

test "native resolver authority prefix routes exclude ten thousand unrelated namespaces" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    try store.ensureHeadRoutes();
    for (0..10000) |i| try store.adoptNamespace(.{ .key_template = try std.fmt.allocPrint(a, "scope-{d}/{{{{_key}}}}", .{i}) });
    try store.adoptNamespace(.{ .key_template = "{{_key}}" });
    memory.reads = 0;
    memory.scans = 0;
    memory.visited = 0;
    const matches = try store.matchingHeads("scope-123/item");
    try std.testing.expectEqual(@as(usize, 2), matches.len);
    try std.testing.expect(memory.reads < 32 and memory.scans <= 2 and memory.visited == 2);
}

test "resolver curation shared profile overlays preserve composed graph validation" {
    const Reference = struct {
        fn validate(a: A, namespace: policy.Namespace, static: []const resolver.KeyAlias, suppress: []const u8) !void {
            var curated: std.StringHashMapUnmanaged(void) = .empty;
            var removed: std.StringHashMapUnmanaged(void) = .empty;
            for (namespace.merges) |alias| try curated.put(a, alias.key, {});
            for (namespace.state.removed_aliases) |key| try removed.put(a, key, {});
            var aliases: std.ArrayList(resolver.KeyAlias) = .empty;
            for (static) |alias| if (!curated.contains(alias.key) and !removed.contains(alias.key)) try aliases.append(a, alias);
            try aliases.appendSlice(a, namespace.merges);
            _ = try c.RedirectIndex.init(a, static);
            _ = try c.RedirectIndex.init(a, namespace.merges);
            const redirects = try c.RedirectIndex.init(a, aliases.items);
            var negative = try c.Index.init(a, suppress, try encode(a, namespace.state));
            try negative.bindRedirects(a, redirects);
            for (namespace.state.distinct) |pair| if (std.mem.eql(u8, redirects.canonical(pair.from), redirects.canonical(pair.to))) return error.ResolverDistinctPair;
            for (aliases.items) |alias| {
                const target = redirects.canonical(alias.target);
                if (negative.keySuppressed(target)) return error.SuppressedResolverRedirectTarget;
                if (try negative.pinnedDistinct(a, alias.key, target)) return error.ResolverDistinctPair;
            }
        }
    };
    var rng = std.Random.DefaultPrng.init(93821);
    const random = rng.random();
    for (0..256) |_| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var names: [32][]const u8 = undefined;
        for (&names, 0..) |*name, i| name.* = try std.fmt.allocPrint(a, "n{d}", .{i});
        var curated: [16]resolver.KeyAlias = undefined;
        for (&curated, 0..) |*alias, i| alias.* = .{ .key = names[i], .target = names[random.intRangeAtMost(usize, i + 1, 31)] };
        var static: [8]resolver.KeyAlias = undefined;
        for (&static, 0..) |*alias, i| alias.* = .{ .key = names[i + 16], .target = names[random.uintLessThan(usize, 32)] };
        var pairs: [4]c.Pair = undefined;
        for (&pairs) |*pair| pair.* = .{ .from = names[random.uintLessThan(usize, 32)], .to = names[random.uintLessThan(usize, 32)] };
        var suppressed: [3][]const u8 = undefined;
        var local_keys: [3][]const u8 = undefined;
        var removed: [3][]const u8 = undefined;
        for (&suppressed, &local_keys, &removed) |*global, *local_key, *tombstone| {
            global.* = names[random.uintLessThan(usize, 32)];
            local_key.* = names[random.uintLessThan(usize, 32)];
            tombstone.* = names[random.uintLessThan(usize, 32)];
        }
        const namespace: policy.Namespace = .{ .key_template = "n{{_key}}", .merges = &curated, .state = .{ .distinct = &pairs, .suppress = .{ .keys = &suppressed }, .removed_aliases = &removed } };
        const suppress = try encode(a, .{ .keys = local_keys });
        const expected = if (Reference.validate(a, namespace, &static, suppress)) |_| true else |_| false;
        const actual = if (validateProfile(a, namespace, &static, suppress)) |_| true else |_| false;
        try std.testing.expectEqual(expected, actual);
    }
}

test "resolver curation shared profile validation keeps a large curated graph once" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const merged = try a.alloc(resolver.KeyAlias, 500);
    for (merged, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "base-{d}", .{i}), .target = "terminal" };
    const profiles = try a.alloc(AliasProfile, 500);
    for (profiles, 0..) |*profile, i| profile.* = .{ .id = try std.fmt.allocPrint(a, "profile-{d}", .{i}), .key_template = "{{_key}}", .aliases = &.{}, .suppress_json = try encode(a, .{ .keys = &[_][]const u8{try std.fmt.allocPrint(a, "unrelated-{d}", .{i})} }) };
    const buffer = try a.alloc(u8, 2 * 1024 * 1024);
    var bounded = std.heap.FixedBufferAllocator.init(buffer);
    try validateProfiles(bounded.allocator(), .{ .key_template = "{{_key}}", .merges = merged }, profiles);
}

test "resolver curation masked static edges do not expand unrelated components or suppression rules" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const backend: Backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = backend };
    const Txn = struct {
        memory: *TestMemory,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.memory.rows.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, raw: []const u8) !void {
            try TestMemory.put(self.memory, self.memory.a, key, raw);
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            try TestMemory.remove(self.memory, self.memory.a, key);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    const template = "entity/{{_key}}";
    const aliases = try a.alloc(resolver.KeyAlias, 17001);
    aliases[0] = .{ .key = "entity/seed", .target = "entity/hub" };
    for (aliases[1..], 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/noise-{d}", .{i}), .target = "entity/hub" };
    const keys = try a.alloc([]const u8, 10001);
    keys[0] = "entity/terminal";
    for (keys[1..], 0..) |*key, i| key.* = try std.fmt.allocPrint(a, "entity/blocked-{d}", .{i});
    const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases, .suppress_json = try encode(a, .{ .keys = keys, .texts = &.{"irrelevant text policy"} }) };
    try static_profiles.synchronize(a, &txn, "profiles/", &.{}, &.{definition});
    var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .removed_aliases = &.{"entity/seed"} } };
    try store.adoptNamespace(namespace);
    var reader: static_profiles.Reader = .{ .a = a, .backend = backend, .key_template = template, .root = "profiles/" };
    const removed = (try store.componentIndexed(template, &.{"entity/seed"}, reader.lookup())).?;
    try std.testing.expectEqual(@as(usize, 0), removed.namespace.merges.len);
    try std.testing.expectEqual(@as(usize, 0), reader.visits);
    const empty = try reader.values();
    try std.testing.expectEqual(@as(usize, 1), empty.len);
    try std.testing.expectEqualStrings("", empty[0].suppress_json);
    namespace.revision = 2;
    namespace.merges = &.{.{ .key = "entity/seed", .target = "entity/terminal" }};
    namespace.state.removed_aliases = &.{};
    try store.apply(try difference(a, removed.namespace, namespace));
    reader = .{ .a = a, .backend = backend, .key_template = template, .root = "profiles/" };
    const redirected = (try store.componentIndexed(template, &.{"entity/seed"}, reader.lookup())).?;
    try std.testing.expectEqual(@as(usize, 1), redirected.namespace.merges.len);
    try std.testing.expectEqual(@as(usize, 0), reader.visits);
    const profiles = try reader.values();
    try std.testing.expect(profiles[0].suppress_json.len < 64);
    try std.testing.expectError(error.SuppressedResolverRedirectTarget, validateProfiles(a, redirected.namespace, profiles));
}

test "resolver curation proposal headers preserve evidence without reading or rewriting bodies" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const evidence = try a.alloc(u8, 12 * 1024);
    @memset(evidence, 'x');
    var evidence_object: std.json.ObjectMap = .empty;
    try evidence_object.put(a, "text", .{ .string = evidence });
    const plan = try c.propose(a, .{}, &.{}, .{ .action = .merge, .key = "entity/a", .into = "entity/b", .evidence = .{ .object = evidence_object } }, .{});
    const proposal_value = plan.state.proposals[0];
    try store.applyQueue(.{ .expected_revision = 0, .revision = 1, .proposals = plan.state.proposals });
    const body_path = try store.proposalPath(proposal_value.id);
    const original_body = memory.rows.get(body_path).?;
    const original_bytes = (try store.queueHead()).bytes;
    memory.read_bytes = 0;
    memory.write_bytes = 0;
    const headers = try store.proposalHeadersForKeys(&.{"entity/a"});
    try std.testing.expectEqual(@as(usize, 1), headers.proposals.len);
    try std.testing.expect(headers.proposals[0].evidence == null);
    const decision = try c.decide(a, headers, &.{}, &.{}, proposal_value.id, .{ .accept = false }, .{});
    try store.applyQueue(.{ .expected_revision = 1, .revision = 2, .proposals = decision.state.proposals, .retain_evidence = true });
    try std.testing.expect(memory.read_bytes < 8 * 1024);
    try std.testing.expect(memory.write_bytes < 4 * 1024);
    try std.testing.expect(original_body.ptr == memory.rows.get(body_path).?.ptr);
    const result = (try store.proposal(proposal_value.id)).?;
    try std.testing.expectEqual(c.ProposalState.rejected, result.state);
    try std.testing.expectEqualStrings(evidence, result.evidence.?.object.get("text").?.string);
    memory.read_bytes = 0;
    const page = try store.proposalPage(null, null, null, 1);
    try std.testing.expectEqual(c.ProposalState.rejected, page.proposals[0].state);
    try std.testing.expectEqualStrings(evidence, page.proposals[0].evidence.?.object.get("text").?.string);
    try std.testing.expect(memory.read_bytes < 4 * 1024);
    try std.testing.expectEqual(c.proposalCapacityBytes(.{ .proposals = &.{result} }), (try store.queueHead()).bytes);
    try std.testing.expect((try store.queueHead()).bytes < original_bytes);
    var forged = decision.state.proposals[0];
    forged.confidence = 0.5;
    try std.testing.expectError(error.ResolverCurationConflict, store.applyQueue(.{ .expected_revision = 2, .revision = 3, .proposals = &.{forged}, .retain_evidence = true }));
    // A touched legacy row acquires a header without rewriting its body.
    _ = memory.rows.remove(try store.proposalHeaderPath(proposal_value.id));
    var legacy_head = try store.queueHead();
    legacy_head.bytes = original_bytes;
    try TestMemory.put(&memory, a, try store.keyPath("queue/head"), try encode(a, legacy_head));
    try std.testing.expectEqual(c.ProposalState.open, (try store.proposalHeader(proposal_value.id)).?.state);
    try store.applyQueue(.{ .expected_revision = 2, .revision = 3, .proposals = decision.state.proposals, .retain_evidence = true });
    try std.testing.expect(original_body.ptr == memory.rows.get(body_path).?.ptr);
    try std.testing.expectEqual(c.ProposalState.rejected, (try store.proposal(proposal_value.id)).?.state);
    try std.testing.expectEqual(c.proposalCapacityBytes(.{ .proposals = &.{result} }), (try store.queueHead()).bytes);
}

test "native resolver mention policy is indexed and includes current normalized text suppression" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    var aliases: [10000]resolver.KeyAlias = undefined;
    for (&aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/a{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/b{d}", .{i}) };
    try store.apply(try difference(a, .{ .key_template = template }, .{ .key_template = template, .revision = 1, .merges = &aliases, .state = .{ .suppress = .{ .texts = &.{"noise"} } } }));
    memory.reads = 0;
    memory.visited = 0;
    const current = (try store.mentionPolicy(template, "entity/a42", "Noise", &.{})).?;
    try std.testing.expectEqual(@as(usize, 1), current.namespace.merges.len);
    try std.testing.expectEqualStrings("entity/b42", current.namespace.merges[0].target);
    const index = try c.Index.init(a, "", try encode(a, current.namespace.state));
    try std.testing.expectEqual(c.Reason.text, (try index.textReason(a, "Noise")).?);
    try std.testing.expect(memory.reads < 40 and memory.visited < 8);
}

test "resolver curation queue scans deduplicate repeated canonical keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan }, .prefix = "authority/" };
    for (0..64) |i| {
        const plan = try c.propose(a, .{}, &.{}, .{ .action = .merge, .key = "entity/root", .into = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) }, .{});
        try store.applyQueue(.{ .expected_revision = i, .revision = i + 1, .proposals = plan.state.proposals });
    }
    memory.scans = 0;
    memory.visited = 0;
    const once = try store.proposalHeadersForKeys(&.{"entity/root"});
    std.debug.print("QUEUE unique_key scans={d} rows={d} proposals={d}\n", .{ memory.scans, memory.visited, once.proposals.len });
    const repeated = try a.alloc([]const u8, 1000);
    @memset(repeated, "entity/root");
    memory.scans = 0;
    memory.visited = 0;
    const amplified = try store.proposalHeadersForKeys(repeated);
    std.debug.print("QUEUE repeated_key scans={d} rows={d} proposals={d}\n", .{ memory.scans, memory.visited, amplified.proposals.len });
    try std.testing.expectEqual(@as(usize, 64), memory.visited);
    try std.testing.expectEqual(@as(usize, 1), memory.scans);
}
test "resolver curation sparse profiles skip ten thousand unrelated heads" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const Txn = struct {
        memory: *TestMemory,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.memory.rows.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, raw: []const u8) !void {
            try TestMemory.put(self.memory, self.memory.a, key, raw);
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            try TestMemory.remove(self.memory, self.memory.a, key);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    const template = "entity/{{_key}}";
    for (0..10000) |i| {
        const definition: static_profiles.Definition = .{ .key_template = template, .suppress_json = try std.fmt.allocPrint(a, "{{\"keys\":[\"entity/unrelated-{d}\"]}}", .{i}) };
        try static_profiles.synchronize(a, &txn, "profiles/", &.{}, &.{definition});
    }
    const store: Store = .{ .a = a, .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan }, .prefix = "authority/" };
    try store.adoptNamespace(.{ .key_template = template, .revision = 1 });
    var reader: static_profiles.Reader = .{ .a = a, .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan }, .key_template = template, .root = "profiles/" };
    memory.visited = 0;
    memory.scans = 0;
    _ = try store.componentIndexed(template, &.{"entity/seed"}, reader.lookup());
    const profiles = try reader.values();
    const encoded = try std.json.Stringify.valueAlloc(a, profiles, .{});
    std.debug.print("PROFILES population=10000 returned={d} edges={d} scans={d} rows={d} payload_bytes={d}\n", .{ profiles.len, reader.visits, memory.scans, memory.visited, encoded.len });
    try std.testing.expectEqual(@as(usize, 0), memory.visited);
    try std.testing.expectEqual(@as(usize, 1), profiles.len);
    try std.testing.expect(encoded.len < 256);
    var invalid: @import("resolver_namespace.zig").Namespace = .{ .key_template = template, .merges = &.{.{ .key = "entity/from", .target = "entity/seed" }}, .state = .{ .suppress = .{ .keys = &.{"entity/seed"} } } };
    try std.testing.expectError(error.SuppressedResolverRedirectTarget, validateProfiles(a, invalid, profiles));
    const relevant: static_profiles.Definition = .{ .key_template = template, .suppress_json = "{\"keys\":[\"entity/seed\"]}" };
    try static_profiles.synchronize(a, &txn, "profiles/", &.{}, &.{relevant});
    // Shared declarations count as one profile. Retiring one reference must
    // retain its negative rule and must not remove the empty-overlay witness.
    try static_profiles.synchronize(a, &txn, "profiles/", &.{}, &.{relevant});
    try static_profiles.synchronize(a, &txn, "profiles/", &.{relevant}, &.{});
    reader = .{ .a = a, .backend = store.backend, .key_template = template, .root = "profiles/" };
    _ = try store.componentIndexed(template, &.{"entity/seed"}, reader.lookup());
    const incident = try reader.values();
    try std.testing.expectEqual(@as(usize, 2), incident.len);
    invalid.state = .{};
    try std.testing.expectError(error.SuppressedResolverRedirectTarget, validateProfiles(a, invalid, incident));
    try static_profiles.synchronize(a, &txn, "profiles/", &.{relevant}, &.{});
    reader = .{ .a = a, .backend = store.backend, .key_template = template, .root = "profiles/" };
    _ = try store.componentIndexed(template, &.{"entity/seed"}, reader.lookup());
    try validateProfiles(a, invalid, try reader.values());
}

test "resolver curation query policy bounds fan in and composes static overrides and text rules" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    var aliases: [10000]resolver.KeyAlias = undefined;
    for (&aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/a{d}", .{i}), .target = "entity/hub" };
    const namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &aliases, .state = .{ .suppress = .{ .keys = &.{"entity/hub"}, .texts = &.{"noise"} }, .removed_aliases = &.{"entity/restored"} } };
    try store.apply(try difference(a, .{ .key_template = template }, namespace));
    memory.reads = 0;
    memory.visited = 0;
    memory.scans = 0;
    const point = (try store.mentionPolicy(template, "entity/hub", "Noise", &.{})).?;
    try std.testing.expectEqual(@as(usize, 0), point.namespace.merges.len);
    try std.testing.expect(memory.reads < 12 and memory.scans == 0);
    memory.reads = 0;
    memory.visited = 0;
    memory.scans = 0;
    const current = (try store.queryPolicy(template, &.{ "entity/static", "entity/restored" }, &.{ "Noise", "NOISE" }, &.{ .{ .key = "entity/static", .target = "entity/a42" }, .{ .key = "entity/restored", .target = "entity/a43" } }, true)).?;
    try std.testing.expectEqual(@as(usize, 1), current.namespace.merges.len);
    try std.testing.expectEqualStrings("entity/a42", current.namespace.merges[0].key);
    try std.testing.expectEqual(@as(usize, 1), current.namespace.state.suppress.texts.len);
    try std.testing.expectEqual(@as(usize, 1), current.namespace.state.removed_aliases.len);
    try std.testing.expect(memory.reads < 32 and memory.scans == 0 and memory.visited == 0);
    std.debug.print("query policy 10k incoming aliases: gets={d} scans={d} rows={d}\n", .{ memory.reads, memory.scans, memory.visited });
}

test "resolver curation query policy retains pins through incoming and static alias composition" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    const namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{.{ .key = "entity/alias", .target = "entity/hub" }}, .state = .{ .distinct = &.{.{ .from = "entity/alias", .to = "entity/other" }} } };
    try store.apply(try difference(a, .{ .key_template = template }, namespace));
    const current = (try store.queryPolicy(template, &.{ "entity/hub", "entity/candidate" }, &.{}, &.{.{ .key = "entity/other", .target = "entity/candidate" }}, true)).?;
    const redirects = try c.RedirectIndex.init(a, &.{ .{ .key = "entity/alias", .target = "entity/hub" }, .{ .key = "entity/other", .target = "entity/candidate" } });
    var index = try c.Index.init(a, "", try encode(a, current.namespace.state));
    try index.bindRedirects(a, redirects);
    try std.testing.expect(try index.pinnedDistinct(a, "entity/hub", "entity/candidate"));
}

test "native resolver authority static pin joins skip ten thousand unrelated configured aliases" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = &.{.{ .from = "entity/unrelated-a", .to = "entity/unrelated-b" }} } };
    try store.apply(try difference(a, .{ .key_template = template }, namespace));
    const aliases = try a.alloc(resolver.KeyAlias, 10002);
    for (aliases[0..10000], 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/noise-{d}", .{i}), .target = "entity/noise-hub" };
    aliases[10000] = .{ .key = "entity/static", .target = "entity/middle" };
    aliases[10001] = .{ .key = "entity/middle", .target = "entity/hub" };
    memory.reads = 0;
    memory.scans = 0;
    memory.visited = 0;
    const empty = (try store.queryPolicy(template, &.{"entity/hub"}, &.{}, aliases, true)).?;
    try std.testing.expectEqual(@as(usize, 0), empty.namespace.state.distinct.len);
    try std.testing.expect(memory.reads < 32 and memory.scans <= 8 and memory.visited <= 4);
    std.debug.print("STATIC PIN JOIN aliases=10002 gets={d} seeks={d} rows={d}\n", .{ memory.reads, memory.scans, memory.visited });
    // A native pin branch may enter a configured chain before reaching the
    // requested canonical key. Overrides and removed static edges stay live.
    var next = namespace;
    next.revision += 1;
    next.merges = &.{.{ .key = "entity/pinned", .target = "entity/static" }};
    next.state.distinct = &.{.{ .from = "entity/pinned", .to = "entity/candidate" }};
    try store.apply(try difference(a, namespace, next));
    namespace = next;
    const incident = (try store.queryPolicy(template, &.{ "entity/hub", "entity/candidate" }, &.{}, aliases, true)).?;
    try std.testing.expectEqual(@as(usize, 1), incident.namespace.state.distinct.len);
    next.revision += 1;
    next.merges = &.{ .{ .key = "entity/pinned", .target = "entity/static" }, .{ .key = "entity/middle", .target = "entity/other" } };
    try store.apply(try difference(a, namespace, next));
    namespace = next;
    try std.testing.expectEqual(@as(usize, 0), (try store.queryPolicy(template, &.{"entity/hub"}, &.{}, aliases, true)).?.namespace.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 1), (try store.queryPolicy(template, &.{"entity/other"}, &.{}, aliases, true)).?.namespace.state.distinct.len);
    next.revision += 1;
    next.state.removed_aliases = &.{"entity/static"};
    try store.apply(try difference(a, namespace, next));
    try std.testing.expectEqual(@as(usize, 0), (try store.queryPolicy(template, &.{"entity/other"}, &.{}, aliases, true)).?.namespace.state.distinct.len);
}

test "native resolver authority prepared canonical pins bound dense unrelated queries and fence mutations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "prepared/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    const aliases = try a.alloc(resolver.KeyAlias, 1000);
    const pairs = try a.alloc(c.Pair, 1000);
    for (aliases, pairs, 0..) |*alias, *pair, i| {
        alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/prepared-{d}", .{i}), .target = "entity/unrelated-root" };
        pair.* = .{ .from = alias.key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
    }
    var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs, .suppress = .{ .texts = &.{"blocked"} } } };
    try store.adoptNamespace(namespace);
    var loader: Store.InlineQueryProfile = .{ .aliases = aliases };
    const profile: Store.QueryProfile = .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a), .ptr = &loader, .load = Store.InlineQueryProfile.load, .estimated_bytes = loader.estimate() };
    memory.reads = 0;
    memory.scans = 0;
    memory.visited = 0;
    const prepared = (try store.prepareQuery(template, profile, true)).?;
    defer prepared.release();
    try std.testing.expect(memory.scans <= 12);
    try std.testing.expectEqual(@as(usize, 1000), memory.visited);
    memory.reads = 0;
    memory.scans = 0;
    memory.visited = 0;
    for (0..100) |_| {
        const empty = try store.queryPrepared(prepared, &.{"entity/seed"}, &.{}, true);
        try std.testing.expect(empty.namespace.merges.len == 0 and empty.namespace.state.distinct.len == 0);
    }
    try std.testing.expectEqual(@as(usize, 400), memory.reads);
    try std.testing.expect(memory.scans == 0 and memory.visited == 0);
    std.debug.print("PREPARED PINS aliases=1000 pins=1000 pages=100 gets={d} scans=0 rows=0\n", .{memory.reads});
    const incident = try store.queryPrepared(prepared, &.{"entity/other-17"}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 1), incident.namespace.state.distinct.len);
    memory.reads = 0;
    memory.scans = 0;
    const text = (try store.queryPolicy(template, &.{}, &.{"BLOCKED"}, aliases, true)).?;
    try std.testing.expectEqual(@as(usize, 1), text.namespace.state.suppress.texts.len);
    try std.testing.expectEqual(@as(usize, 2), memory.reads);
    try std.testing.expectEqual(@as(usize, 0), memory.scans);
    // Text-only edits retain graph identity but still fence old full heads.
    var churn_rows: usize = 0;
    var churn_scans: usize = 0;
    for (0..10) |i| {
        var edited = namespace;
        edited.revision += 1;
        edited.state.suppress.texts = &.{try std.fmt.allocPrint(a, "noise text {d}", .{i})};
        try store.apply(try difference(a, namespace, edited));
        namespace = edited;
        memory.visited = 0;
        memory.scans = 0;
        const current = (try store.prepareQuery(template, profile, true)).?;
        defer current.release();
        try std.testing.expect(current.lease.?.context == prepared.lease.?.context);
        _ = try store.queryPrepared(current, &.{"entity/seed"}, &.{}, true);
        churn_rows += memory.visited;
        churn_scans += memory.scans;
        try std.testing.expectError(error.ResolverCurationConflict, store.queryPrepared(prepared, &.{"entity/seed"}, &.{}, true));
    }
    try std.testing.expectEqual(@as(usize, 0), churn_rows);
    try std.testing.expectEqual(@as(usize, 0), churn_scans);
    std.debug.print("TEXT EDIT CHURN edits=10 unrelated_pins=1000 rows={d} scans={d}\n", .{ churn_rows, churn_scans });
    var next = namespace;
    next.revision += 1;
    next.merges = &.{.{ .key = aliases[17].key, .target = "entity/new-root" }};
    try store.apply(try difference(a, namespace, next));
    namespace = next;
    try std.testing.expectError(error.ResolverCurationConflict, store.queryPrepared(prepared, &.{"entity/seed"}, &.{}, true));
    const moved = (try store.prepareQuery(template, profile, true)).?;
    defer moved.release();
    try std.testing.expectEqual(@as(usize, 1), (try store.queryPrepared(moved, &.{"entity/new-root"}, &.{}, true)).namespace.state.distinct.len);
    next.revision += 1;
    next.merges = &.{};
    next.state.removed_aliases = &.{aliases[17].key};
    try store.apply(try difference(a, namespace, next));
    namespace = next;
    const removed = (try store.prepareQuery(template, profile, true)).?;
    defer removed.release();
    try std.testing.expectEqual(@as(usize, 0), (try store.queryPrepared(removed, &.{"entity/new-root"}, &.{}, true)).namespace.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 1), (try store.queryPrepared(removed, &.{aliases[17].key}, &.{}, true)).namespace.state.distinct.len);
    next.revision += 1;
    next.state.suppress.keys = &.{"entity/other-17"};
    try store.apply(try difference(a, namespace, next));
    try std.testing.expectError(error.ResolverCurationConflict, store.queryPrepared(removed, &.{"entity/other-17"}, &.{}, true));
    const suppressed = (try store.prepareQuery(template, profile, true)).?;
    defer suppressed.release();
    try std.testing.expectEqualStrings("entity/other-17", (try store.queryPrepared(suppressed, &.{"entity/other-17"}, &.{}, true)).namespace.state.suppress.keys[0]);
    // Released zero-fingerprint heads do not qualify shared composed entries.
    var head = (try store.head(template)).?;
    head.fingerprint = @splat(0);
    try store.backend.put(store.backend.ptr, a, try store.headPath(template), try encode(a, head));
    const legacy = (try store.prepareQuery(template, profile, true)).?;
    defer legacy.release();
    const another = (try store.prepareQuery(template, profile, true)).?;
    defer another.release();
    try std.testing.expect(legacy.indexed and another.indexed);
    try std.testing.expect(!legacy.lease.?.context.composed and !another.lease.?.context.composed);
}

test "resolver curation query policy unions bounded batches with a head lineage fence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const h: Head = .{ .key_template = "entity/{{_key}}", .revision = 7, .min_text_chars = 2, .fingerprint = @splat(1) };
    const part: Snapshot = .{ .configured_suppression = .{ .keys = &.{"entity/configured"}, .texts = &.{"configured noise"}, .min_text_chars = 4 }, .head = h, .namespace = .{ .key_template = h.key_template, .revision = 7, .merges = &.{.{ .key = "entity/a", .target = "entity/b" }}, .state = .{ .suppress = .{ .texts = &.{"noise"} } } } };
    const joined = try unionQueryPolicies(a, &.{ part, part });
    try std.testing.expectEqual(@as(usize, 1), joined.namespace.merges.len);
    try std.testing.expectEqual(@as(usize, 1), joined.namespace.state.suppress.texts.len);
    try std.testing.expectEqual(@as(u32, 2), joined.namespace.state.suppress.min_text_chars);
    try std.testing.expectEqual(@as(usize, 1), joined.configured_suppression.?.keys.len);
    try std.testing.expectEqualStrings("configured noise", joined.configured_suppression.?.texts[0]);
    try std.testing.expectEqual(@as(u32, 4), joined.configured_suppression.?.min_text_chars);
    var legacy = part;
    legacy.configured_suppression = null;
    try std.testing.expectError(error.ResolverCurationConflict, unionQueryPolicies(a, &.{ part, legacy }));
    var divergent = part;
    divergent.head.fingerprint = @splat(2);
    try std.testing.expectError(error.ResolverCurationConflict, unionQueryPolicies(a, &.{ part, divergent }));
}

test "resolver curation query policy wide native page union attaches without a query wide node cap" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "wide-attachment/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_entity.text}}";
    const aliases = try a.alloc(resolver.KeyAlias, 9000);
    const seeds = try a.alloc([]const u8, aliases.len);
    for (aliases, seeds, 0..) |*edge, *seed, i| {
        edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/a-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/b-{d}", .{i}) };
        seed.* = edge.key;
    }
    try store.adoptNamespace(.{ .key_template = template, .revision = 1 });
    var loader: Store.InlineQueryProfile = .{ .aliases = aliases };
    const id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a);
    const prepared = (try store.prepareQuery(template, .{ .id = id, .ptr = &loader, .load = Store.InlineQueryProfile.load, .estimated_bytes = loader.estimate() }, false)).?;
    defer prepared.release();
    var joined: QueryPolicyUnion = .{};
    var offset: usize = 0;
    var pages: usize = 0;
    while (offset < seeds.len) {
        var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer scratch.deinit();
        var page_store = store;
        page_store.a = scratch.allocator();
        const end = @min(offset + 1024, seeds.len);
        try joined.add(a, scratch.allocator(), try page_store.queryPrepared(prepared, seeds[offset..end], &.{}, false));
        offset = end;
        pages += 1;
    }
    const snapshot = try joined.finish();
    var target = try resolver.Resolver.initFromParts(a, "entities", template, .{}, false, "");
    defer target.deinit();
    try target.installQueryPolicy(a, id, aliases, "", snapshot.namespace.merges, try encode(a, snapshot.namespace.state), seeds);
    for (aliases) |edge| try std.testing.expectEqualStrings(edge.target, target.alias_index.?.canonical(edge.key));
    try std.testing.expectEqual(@as(usize, 9), pages);
    std.debug.print("NATIVE PAGE ATTACHMENT seeds=9000 pages=9 canonical_results=9000\n", .{});
}

test "resolver curation query policy unrelated pins never expand a large reverse component" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    var aliases: [10000]resolver.KeyAlias = undefined;
    for (&aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/a{d}", .{i}), .target = "entity/hub" };
    var state: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &aliases, .state = .{ .distinct = &.{.{ .from = "entity/unrelated-a", .to = "entity/unrelated-b" }} } };
    try store.apply(try difference(a, .{ .key_template = template }, state));
    memory.reads = 0;
    memory.scans = 0;
    memory.visited = 0;
    const sparse = (try store.queryPolicy(template, &.{"entity/hub"}, &.{}, &.{}, true)).?;
    try std.testing.expectEqual(@as(usize, 0), sparse.namespace.merges.len);
    try std.testing.expect(memory.reads < 16 and memory.scans == 0 and memory.visited == 0);
    std.debug.print("PIN INDEX 10k incoming unrelated: gets={d} scans={d} rows={d}\n", .{ memory.reads, memory.scans, memory.visited });
    const previous = state;
    state.revision = 2;
    state.state.distinct = &.{.{ .from = "entity/a42", .to = "entity/other" }};
    try store.apply(try difference(a, previous, state));
    memory.reads = 0;
    memory.scans = 0;
    memory.visited = 0;
    const incident = (try store.queryPolicy(template, &.{"entity/hub"}, &.{}, &.{}, true)).?;
    try std.testing.expectEqual(@as(usize, 1), incident.namespace.merges.len);
    try std.testing.expectEqual(@as(usize, 1), incident.namespace.state.distinct.len);
    try std.testing.expect(memory.visited < 8 and memory.reads < 40);
    const next = state;
    state.revision = 3;
    const changed = try a.dupe(resolver.KeyAlias, &aliases);
    changed[42].target = "entity/final";
    state.merges = changed;
    try store.apply(try difference(a, next, state));
    const old = (try store.queryPolicy(template, &.{"entity/hub"}, &.{}, &.{}, true)).?;
    const moved = (try store.queryPolicy(template, &.{"entity/final"}, &.{}, &.{}, true)).?;
    try std.testing.expectEqual(@as(usize, 0), old.namespace.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 1), moved.namespace.state.distinct.len);
    const last = state;
    state.revision = 4;
    state.state.distinct = &.{};
    try store.apply(try difference(a, last, state));
    try std.testing.expectEqual(@as(u64, 0), try store.pinCount(template, "entity/final"));
    try std.testing.expectEqual(@as(u64, 0), try store.pinCount(template, "entity/a42"));
}

test "resolver curation pin index backfills legacy namespaces before publishing readiness" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    try store.apply(try difference(a, .{ .key_template = template }, .{ .key_template = template, .revision = 1, .merges = &.{ .{ .key = "entity/a", .target = "entity/b" }, .{ .key = "entity/b", .target = "entity/c" } }, .state = .{ .distinct = &.{.{ .from = "entity/a", .to = "entity/other" }} } }));
    var derived: std.ArrayList([]const u8) = .empty;
    var rows = memory.rows.iterator();
    while (rows.next()) |row| {
        try validatePortableRow(a, row.key_ptr.*, row.value_ptr.*);
        if (std.mem.startsWith(u8, row.key_ptr.*[lite_prefix.len..], "pin-")) try derived.append(a, row.key_ptr.*);
    }
    try std.testing.expect(derived.items.len > 3);
    for (derived.items) |key| _ = memory.rows.remove(key);
    try std.testing.expect(memory.rows.get(try store.pinReadyPath(template)) == null);
    try store.ensurePinIndex(template);
    try std.testing.expectEqual(@as(u64, 1), try store.pinCount(template, "entity/c"));
    const result = (try store.queryPolicy(template, &.{"entity/c"}, &.{}, &.{}, true)).?;
    try std.testing.expectEqual(@as(usize, 1), result.namespace.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 2), result.namespace.merges.len);
    const writes = memory.writes;
    try store.ensurePinIndex(template);
    try std.testing.expectEqual(writes, memory.writes);
}

test "resolver curation pin index preserves rotated paths static overlays and tombstones" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "{{_key}}";
    const statics: []const resolver.KeyAlias = &.{ .{ .key = "a", .target = "s" }, .{ .key = "c", .target = "t" }, .{ .key = "y", .target = "q" } };
    const stages: []const policy.Namespace = &.{
        .{ .key_template = template, .revision = 1, .merges = &.{ .{ .key = "a", .target = "b" }, .{ .key = "b", .target = "c" }, .{ .key = "x", .target = "y" } }, .state = .{ .distinct = &.{.{ .from = "a", .to = "x" }} } },
        .{ .key_template = template, .revision = 2, .merges = &.{ .{ .key = "b", .target = "a" }, .{ .key = "x", .target = "y" } }, .state = .{ .distinct = &.{ .{ .from = "a", .to = "x" }, .{ .from = "b", .to = "z" } } } },
        .{ .key_template = template, .revision = 3, .merges = &.{ .{ .key = "b", .target = "a" }, .{ .key = "x", .target = "y" } }, .state = .{ .distinct = &.{ .{ .from = "a", .to = "x" }, .{ .from = "b", .to = "z" } }, .removed_aliases = &.{"a"} } },
    };
    const Build = struct {
        fn index(alloc: A, scope: Snapshot, static: []const resolver.KeyAlias) !c.Index {
            var negatives = try c.Index.init(alloc, "", try encode(alloc, scope.namespace.state));
            var edges: std.StringArrayHashMapUnmanaged([]const u8) = .empty;
            for (static) |alias| if (!negatives.removed_aliases.contains(alias.key)) try edges.put(alloc, alias.key, alias.target);
            for (scope.namespace.merges) |alias| try edges.put(alloc, alias.key, alias.target);
            const all = try alloc.alloc(resolver.KeyAlias, edges.count());
            for (edges.keys(), edges.values(), all) |key, target, *alias| alias.* = .{ .key = key, .target = target };
            try negatives.bindRedirects(alloc, try c.RedirectIndex.init(alloc, all));
            return negatives;
        }
    };
    var before: policy.Namespace = .{ .key_template = template };
    for (stages) |state| {
        try store.apply(try difference(a, before, state));
        before = state;
        for ([_][]const u8{ "a", "b", "s", "t", "x", "q", "z" }) |from| for ([_][]const u8{ "a", "b", "s", "t", "x", "q", "z" }) |to| {
            const scoped = (try store.queryPolicy(template, &.{ from, to }, &.{}, statics, true)).?;
            const full = (try store.exportNamespace(template)).?;
            const actual = try Build.index(a, scoped, statics);
            const expected = try Build.index(a, full, statics);
            try std.testing.expectEqual(try expected.pinnedDistinct(a, from, to), try actual.pinnedDistinct(a, from, to));
        };
    }
}

test "resolver curation pin batch propagation is linear across overlapping and rotated paths" {
    for ([_]usize{ 100, 200, 400, 1600 }) |n| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: TestMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = "authority/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
        const pairs = try a.alloc(c.Pair, n);
        const ids = try a.alloc([]const u8, n);
        for (ids, pairs, 0..) |*id, *pair, i| {
            id.* = try std.fmt.allocPrint(a, "node-{d:0>5}", .{i});
            pair.* = .{ .from = id.*, .to = try std.fmt.allocPrint(a, "other-{d:0>5}", .{i}) };
        }
        var before: policy.Namespace = .{ .key_template = "{{_key}}", .revision = 1, .state = .{ .distinct = pairs } };
        try store.adoptNamespace(before);
        const aliases = try a.alloc(resolver.KeyAlias, n - 1);
        for (aliases, 0..) |*alias, j| {
            const i = n - 2 - j;
            alias.* = .{ .key = ids[i], .target = ids[i + 1] };
        }
        var after = before;
        after.revision += 1;
        after.merges = aliases;
        memory.reads = 0;
        memory.writes = 0;
        try store.apply(try difference(a, before, after));
        const reads = memory.reads;
        const writes = memory.writes;
        for (ids, 0..) |id, i| try std.testing.expectEqual(@as(u64, @intCast(i + 1)), try store.pinCount(before.key_template, id));
        try std.testing.expect(reads < n * 32 and writes < n * 16);
        std.debug.print("PIN BATCH n={d} reads={d} writes={d} arena={d}\n", .{ n, reads, writes, arena.queryCapacity() });
        before = after;
        const rotated = try a.alloc(resolver.KeyAlias, n - 1);
        for (rotated, 1..) |*alias, i| alias.* = .{ .key = ids[i], .target = ids[i - 1] };
        after.revision += 1;
        after.merges = rotated;
        try store.apply(try difference(a, before, after));
        for (ids, 0..) |id, i| try std.testing.expectEqual(@as(u64, @intCast(n - i)), try store.pinCount(before.key_template, id));
        before = after;
        after.revision += 1;
        after.merges = &.{};
        try store.apply(try difference(a, before, after));
        for (ids) |id| try std.testing.expectEqual(@as(u64, 1), try store.pinCount(before.key_template, id));
    }
}

test "native resolver authority ownership radix seeks history and resumes revision fenced backfill" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    // Released queues have only the historical hashed-key directory.
    try store.backend.put(&memory, a, try store.keyPath("queue/head"), try encode(a, QueueHead{ .revision = 7, .count = 1000 }));
    const keys = try a.alloc([]const u8, 1000);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(a, "entity/{d}/claim-{d}.fact", .{ i % 10, i });
        try store.backend.put(&memory, a, try store.keyPath(try std.fmt.allocPrint(a, "queue/owned/{s}", .{try hash(a, key.*)})), try encode(a, key.*));
    }
    try std.testing.expect(!try store.ownershipIndexed());
    try std.testing.expect(try store.ownsKeyTemplate("entity/0/{{_key}}.fact"));
    var turns: usize = 0;
    while (true) {
        turns += 1;
        if (try store.advanceOwnershipIndex(37)) break;
        if (turns == 1) {
            // A new writer adds a key behind the hashed backfill cursor. The
            // same transaction updates both the radix indexes and queue cut.
            const request: c.ProposalRequest = .{ .action = .suppress, .key = "before/new" };
            const proposed = try c.propose(a, .{}, &.{}, request, .{});
            try store.applyQueue(.{ .expected_revision = 7, .revision = 8, .proposals = proposed.state.proposals });
        }
    }
    try std.testing.expect(turns <= 28);
    try std.testing.expect(try store.ownershipIndexed());
    try std.testing.expect(try store.ownsKeyTemplate("before/{{_key}}"));
    const visited = memory.visited;
    for (0..1000) |i| try std.testing.expect(!try store.ownsKeyTemplate(try std.fmt.allocPrint(a, "unrelated-{d}/{{{{_key}}}}", .{i})));
    try std.testing.expectEqual(visited, memory.visited);
    const templates = [_][]const u8{ "{{_key}}", "entity/{{_key}}", "entity/0/{{_key}}.fact", "{{_key}}.fact", "entity/{{_key}}.no", "{{_key}}/new", "entity/9/claim-999.fact", "entity/9/claim-998.fact", "before/{{_key}}.fact" };
    for (templates) |template| {
        var expected = policy.keyInNamespace(template, "before/new");
        for (keys) |key| expected = expected or policy.keyInNamespace(template, key);
        try std.testing.expectEqual(expected, try store.ownsKeyTemplate(template));
    }
    // An older writer cannot accidentally certify a stale derived index.
    try store.backend.put(&memory, a, try store.keyPath("queue/head"), try encode(a, QueueHead{ .revision = 9, .count = 1002 }));
    try store.backend.put(&memory, a, try store.keyPath(try std.fmt.allocPrint(a, "queue/owned/{s}", .{try hash(a, "late/old-writer")})), try encode(a, "late/old-writer"));
    try std.testing.expect(!try store.ownershipIndexed());
    try std.testing.expect(try store.ownsKeyTemplate("late/{{_key}}"));
    while (!try store.advanceOwnershipIndex(128)) {}
    try std.testing.expect(try store.ownsKeyTemplate("late/{{_key}}"));
    // Compressed labels accept long keys without producing long physical keys
    // or one trie row for every byte of their common prefix.
    const long = try a.alloc(u8, 8192);
    @memset(long, 'a');
    const index = try store.ownershipIndex("forward");
    const writes = memory.writes;
    try store.indexOwnedKey(long);
    try store.indexOwnedKey(long);
    try std.testing.expect(memory.writes - writes < 32);
    try std.testing.expect(try index.prefix(try Store.Ownership.encodedKey(a, long[0..8000], false)) != null);
    try store.indexOwnedKey("café/東京.fact");
    try std.testing.expect(try store.ownsKeyTemplate("café/{{_key}}.fact"));
    try std.testing.expect(try store.ownsKeyTemplate("{{_key}}東京.fact"));
    var rows = memory.rows.iterator();
    while (rows.next()) |row| {
        try std.testing.expect(row.key_ptr.len < 512);
        if (std.mem.indexOf(u8, row.key_ptr.*, "queue/ownership") != null) try validatePortableRow(a, row.key_ptr.*, row.value_ptr.*);
    }
    std.debug.print("OWNERSHIP history=1000 absent_templates=1000 unrelated_rows=0 backfill_turns={d}\n", .{turns});
}

test "native resolver authority indexed query fallback follows only relevant components and preserves gates" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "indexed-query/", .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const template = "entity/{{_key}}";
    const aliases: []const resolver.KeyAlias = &.{ .{ .key = "entity/static", .target = "entity/middle" }, .{ .key = "entity/middle", .target = "entity/root" }, .{ .key = "entity/removed", .target = "entity/root" } };
    const pairs = try a.alloc(c.Pair, 4001);
    for (pairs[0..4000], 0..) |*pair, i| pair.* = .{ .from = try std.fmt.allocPrint(a, "entity/unrelated-pinned-{d}", .{i}), .to = try std.fmt.allocPrint(a, "entity/unrelated-other-{d}", .{i}) };
    pairs[4000] = .{ .from = "entity/native", .to = "entity/candidate" };
    const namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{ .{ .key = "entity/native", .target = "entity/static" }, .{ .key = "entity/middle", .target = "entity/moved" } }, .state = .{ .distinct = pairs, .removed_aliases = &.{"entity/removed"}, .suppress = .{ .keys = &.{ "entity/moved", "entity/cascaded" }, .entries = &.{.{ .key = "entity/cascaded", .origin = .{ .origin = .cascade, .cascade_from = "entity/absent" } }}, .texts = &.{"blocked"} } } };
    try store.adoptNamespace(namespace);
    var inline_profile: Store.InlineQueryProfile = .{ .aliases = aliases };
    const prepared = (try store.prepareQuery(template, .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a), .ptr = &inline_profile, .load = Store.InlineQueryProfile.load, .estimated_bytes = inline_profile.estimate() }, true)).?;
    defer prepared.release();
    try std.testing.expect(prepared.indexed);
    try std.testing.expect(!prepared.lease.?.context.composed);
    memory.reads = 0;
    memory.scans = 0;
    memory.visited = 0;
    memory.distinct_rows = 0;
    const empty = try store.queryPrepared(prepared, &.{"entity/absent"}, &.{"blocked"}, true);
    try std.testing.expectEqual(@as(usize, 0), empty.namespace.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 0), memory.distinct_rows);
    try std.testing.expect(memory.visited < 16 and memory.reads < 64 and memory.scans < 20);
    std.debug.print("INDEXED QUERY FALLBACK unrelated_pins=4000 gets={d} scans={d} rows={d}\n", .{ memory.reads, memory.scans, memory.visited });
    const current = try store.queryPrepared(prepared, &.{ "entity/moved", "entity/candidate", "entity/removed" }, &.{}, true);
    try std.testing.expectEqual(@as(usize, 1), current.namespace.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 1), current.namespace.state.removed_aliases.len);
    var effective: std.ArrayList(resolver.KeyAlias) = .empty;
    for (aliases) |alias| if (!std.mem.eql(u8, alias.key, "entity/middle") and !std.mem.eql(u8, alias.key, "entity/removed")) try effective.append(a, alias);
    try effective.appendSlice(a, current.namespace.merges);
    const redirects = try c.RedirectIndex.init(a, effective.items);
    var index = try c.Index.init(a, "", try encode(a, current.namespace.state));
    try index.bindRedirects(a, redirects);
    try std.testing.expect(try index.pinnedDistinct(a, "entity/moved", "entity/candidate"));
    try std.testing.expect(index.keySuppressed("entity/moved"));
    memory.visited = 0;
    const rendering = try store.queryPrepared(prepared, &.{"entity/native"}, &.{}, false);
    try std.testing.expectEqual(@as(usize, 0), rendering.namespace.state.distinct.len);
    try std.testing.expectEqual(@as(usize, 0), memory.visited);
    var without_pins = namespace;
    without_pins.revision += 1;
    without_pins.state.distinct = &.{};
    try store.apply(try difference(a, namespace, without_pins));
    const no_pins = (try store.prepareQuery(template, .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a), .ptr = &inline_profile, .load = Store.InlineQueryProfile.load, .estimated_bytes = std.math.maxInt(usize) }, true)).?;
    defer no_pins.release();
    memory.scans = 0;
    _ = try store.queryPrepared(no_pins, &.{"entity/native"}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 0), memory.scans);
}

test "native resolver authority stale derived graph stamps cannot hide older writer mutations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "older-writer/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{.{ .key = "entity/a", .target = "entity/old" }}, .state = .{ .distinct = &.{.{ .from = "entity/a", .to = "entity/other" }} } };
    try store.adoptNamespace(namespace);
    const aliases: []const resolver.KeyAlias = &.{.{ .key = "entity/unused-static", .target = "entity/unused-root" }};
    var inline_profile: Store.InlineQueryProfile = .{ .aliases = aliases };
    const profile: Store.QueryProfile = .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a), .ptr = &inline_profile, .load = Store.InlineQueryProfile.load, .estimated_bytes = inline_profile.estimate() };
    const before = (try store.prepareQuery(template, profile, true)).?;
    defer before.release();
    const saved = (try store.backend.get(store.backend.ptr, a, try store.queryGraphPath(template))).?;
    var next = namespace;
    next.revision += 1;
    next.merges = &.{.{ .key = "entity/a", .target = "entity/new" }};
    try store.apply(try difference(a, namespace, next));
    namespace = next;
    // Simulate an older writer retaining an optional derived stamp it does not
    // know how to maintain. Its complete head still changes atomically.
    try store.backend.put(store.backend.ptr, a, try store.queryGraphPath(template), saved);
    const prepared = (try store.prepareQuery(template, profile, true)).?;
    defer prepared.release();
    try std.testing.expectError(error.ResolverCurationConflict, store.queryPrepared(before, &.{"entity/new"}, &.{}, true));
    const current = try store.queryPrepared(prepared, &.{"entity/new"}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 1), current.namespace.state.distinct.len);
    try std.testing.expectEqualStrings("entity/new", current.namespace.merges[0].target);
    try store.backend.remove(store.backend.ptr, a, try store.queryGraphPath(template));
    const missing = (try store.prepareQuery(template, profile, true)).?;
    defer missing.release();
    try std.testing.expectEqual(@as(usize, 1), (try store.queryPrepared(missing, &.{"entity/new"}, &.{}, true)).namespace.state.distinct.len);
}

test "native resolver authority pin directed fallback skips high degree unpinned alias components" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "pin-directed/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = &.{.{ .from = "entity/unrelated-a", .to = "entity/unrelated-b" }} } };
    try store.adoptNamespace(namespace);
    const aliases = try a.alloc(resolver.KeyAlias, 17000);
    for (aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/branch-{d}", .{i}), .target = "entity/root" };
    var profile: Store.InlineQueryProfile = .{ .aliases = aliases };
    const id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a);
    const fallback = (try store.prepareQuery(template, .{ .id = id, .ptr = &profile, .load = Store.InlineQueryProfile.load, .estimated_bytes = std.math.maxInt(usize) }, true)).?;
    defer fallback.release();
    memory.reads = 0;
    memory.scans = 0;
    const empty = try store.queryPrepared(fallback, &.{"entity/root"}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 0), empty.namespace.state.distinct.len);
    try std.testing.expect(memory.reads < 32 and memory.scans < 16);
    std.debug.print("PIN DIRECTED FALLBACK aliases=17000 gets={d} scans={d} pairs=0\n", .{ memory.reads, memory.scans });
    const Txn = struct {
        memory: *TestMemory,
        pub fn get(self: *@This(), key_: []const u8) ![]const u8 {
            return self.memory.rows.get(key_) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key_: []const u8, value: []const u8) !void {
            return TestMemory.put(self.memory, self.memory.a, key_, value);
        }
        pub fn delete(self: *@This(), key_: []const u8) !void {
            return TestMemory.remove(self.memory, self.memory.a, key_);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    try static_profiles.synchronize(a, &txn, "profile-index/", &.{}, &.{.{ .key_template = template, .aliases = aliases }});
    var indexed: static_profiles.QueryReader = .{ .backend = store.backend, .root = "profile-index/", .profile = id, .indexed = try static_profiles.queryNodesReady(a, &txn, "profile-index/", id) };
    const Indexed = struct {
        fn noLoad(_: *anyopaque, _: A) ![]const resolver.KeyAlias {
            return error.OutOfMemory;
        }
        fn same(a_: A, left: Snapshot, right: Snapshot) !void {
            const left_rows = try flatten(a_, left.namespace);
            const right_rows = try flatten(a_, right.namespace);
            try std.testing.expectEqual(left_rows.len, right_rows.len);
            const joined = try unionNamespaces(a_, left.namespace, right.namespace);
            try std.testing.expectEqual(left_rows.len, (try flatten(a_, joined)).len);
        }
    };
    const remote_profile: Store.QueryProfile = .{ .id = id, .ptr = &profile, .load = Indexed.noLoad, .estimated_bytes = std.math.maxInt(usize), .lookup = indexed.lookup() };
    const remote = (try store.prepareQuery(template, remote_profile, true)).?;
    defer remote.release();
    try std.testing.expect(remote.lease == null);
    try Indexed.same(a, empty, try store.queryPrepared(remote, &.{"entity/root"}, &.{}, true));
    // Loading graph aliases does not retain the profile's unrelated text policy.
    const large_policy = try a.alloc(u8, 2 * 1024 * 1024);
    @memset(large_policy, ' ');
    large_policy[0] = '{';
    large_policy[1] = '}';
    const bounded_definition: static_profiles.Definition = .{ .key_template = template, .aliases = &.{.{ .key = "entity/bounded", .target = "entity/root" }}, .suppress_json = large_policy };
    const bounded_id = try bounded_definition.identity(a);
    try static_profiles.synchronize(a, &txn, "bounded-profile/", &.{}, &.{bounded_definition});
    var buffer: [64 * 1024]u8 = undefined;
    var bounded: std.heap.FixedBufferAllocator = .init(&buffer);
    const loaded = try static_profiles.queryAliases(bounded.allocator(), &txn, "bounded-profile/", bounded_id, template);
    try std.testing.expectEqual(@as(usize, 1), loaded.len);
    try std.testing.expectEqualStrings("entity/bounded", loaded[0].key);
    std.debug.print("QUERY ALIASES unrelated_policy_bytes=2097152 scratch_bytes={d}\n", .{bounded.end_index});
    var next = namespace;
    next.revision += 1;
    next.merges = &.{ .{ .key = aliases[5000].key, .target = "entity/moved" }, .{ .key = "entity/native", .target = aliases[14567].key } };
    next.state.removed_aliases = &.{aliases[100].key};
    next.state.distinct = &.{ .{ .from = aliases[5000].key, .to = "entity/other-moved" }, .{ .from = aliases[14567].key, .to = "entity/other-root" }, .{ .from = "entity/native", .to = "entity/other-native" }, .{ .from = aliases[100].key, .to = "entity/other-removed" } };
    try store.apply(try difference(a, namespace, next));
    namespace = next;
    const current = (try store.prepareQuery(template, .{ .id = id, .ptr = &profile, .load = Store.InlineQueryProfile.load, .estimated_bytes = std.math.maxInt(usize) }, true)).?;
    defer current.release();
    const selected = try store.queryPrepared(current, &.{"entity/root"}, &.{}, true);
    const remote_current = (try store.prepareQuery(template, remote_profile, true)).?;
    defer remote_current.release();
    try Indexed.same(a, selected, try store.queryPrepared(remote_current, &.{"entity/root"}, &.{}, true));
    const moved = try store.queryPrepared(remote_current, &.{"entity/moved"}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 1), moved.namespace.state.distinct.len);
    try std.testing.expectEqualStrings("entity/other-moved", moved.namespace.state.distinct[0].to);
    const restored = try store.queryPrepared(remote_current, &.{aliases[100].key}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 1), restored.namespace.state.distinct.len);
    try std.testing.expectEqualStrings("entity/other-removed", restored.namespace.state.distinct[0].to);
    try std.testing.expectEqual(@as(usize, 2), selected.namespace.state.distinct.len);
    for (selected.namespace.state.distinct) |pair| try std.testing.expect(std.mem.eql(u8, pair.to, "entity/other-root") or std.mem.eql(u8, pair.to, "entity/other-native"));
    // A missing legacy pin directory also preserves result semantics. It
    // streams only distinct records until an authorized writer adopts it.
    try store.backend.remove(store.backend.ptr, a, try store.pinReadyPath(template));
    const legacy = try store.queryPrepared(current, &.{"entity/root"}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 2), legacy.namespace.state.distinct.len);
}

test "native resolver authority shares five thousand profile aliases across graph versions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "shared-base-regression/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    const aliases = try a.alloc(resolver.KeyAlias, 5000);
    for (aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/shared-base-{d}", .{i}), .target = "entity/base-root" };
    const Tracker = struct {
        aliases: []const resolver.KeyAlias,
        loads: usize = 0,
        fn load(ptr: *anyopaque, alloc: A) ![]const resolver.KeyAlias {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.loads += 1;
            var inline_profile: Store.InlineQueryProfile = .{ .aliases = self.aliases };
            return Store.InlineQueryProfile.load(&inline_profile, alloc);
        }
    };
    var tracker: Tracker = .{ .aliases = aliases };
    const profile: Store.QueryProfile = .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a), .ptr = &tracker, .load = Tracker.load, .estimated_bytes = (Store.InlineQueryProfile{ .aliases = aliases }).estimate() };
    var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = &.{.{ .from = "entity/outside-a", .to = "entity/outside-b" }} } };
    try store.adoptNamespace(namespace);
    var bytes: usize = 0;
    var first_nodes: ?[*]const query_context.QueryNode = null;
    for (0..10) |i| {
        if (i != 0) {
            var next = namespace;
            next.revision += 1;
            const changed = try a.alloc(resolver.KeyAlias, 1);
            changed[0] = .{ .key = "entity/native-outside", .target = try std.fmt.allocPrint(a, "entity/outside-{d}", .{i}) };
            next.merges = changed;
            try store.apply(try difference(a, namespace, next));
            namespace = next;
        }
        const prepared = (try store.prepareQuery(template, profile, true)).?;
        defer prepared.release();
        const context = prepared.lease.?.context;
        if (first_nodes) |ptr| try std.testing.expect(ptr == context.nodes.nodes.ptr) else first_nodes = context.nodes.nodes.ptr;
        bytes += context.arena.queryCapacity();
        _ = try store.queryPrepared(prepared, &.{"entity/base-root"}, &.{}, true);
    }
    try std.testing.expectEqual(@as(usize, 1), tracker.loads);
    try std.testing.expect(bytes < 1024 * 1024);
    std.debug.print("SHARED PROFILE aliases=5000 versions=10 loads={d} overlay_arena_bytes={d}\n", .{ tracker.loads, bytes });
}

test "native resolver authority root directed fallback and reusable routing fence graph changes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]usize{ 100, 1000 }) |count| {
        var memory: TestMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = try std.fmt.allocPrint(a, "routing-regression-{d}/", .{count}), .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
        const template = "entity/{{_key}}";
        const aliases = try a.alloc(resolver.KeyAlias, count);
        const natives = try a.alloc(resolver.KeyAlias, count);
        for (aliases, natives, 0..) |*alias, *native, i| {
            alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/routing-branch-{d}", .{i}), .target = "entity/static-root" };
            native.* = .{ .key = alias.key, .target = "entity/curated-root" };
        }
        var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = natives, .state = .{ .distinct = &.{ .{ .from = "entity/pin-a", .to = "entity/pin-b" }, .{ .from = "entity/unconnected", .to = "entity/own-pin" } } } };
        try store.adoptNamespace(namespace);
        var inline_profile: Store.InlineQueryProfile = .{ .aliases = aliases };
        const profile: Store.QueryProfile = .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a), .ptr = &inline_profile, .load = Store.InlineQueryProfile.load, .estimated_bytes = std.math.maxInt(usize) };
        const prepared = (try store.prepareQuery(template, profile, true)).?;
        defer prepared.release();
        memory.reads = 0;
        memory.scans = 0;
        const own = try store.queryPrepared(prepared, &.{"entity/unconnected"}, &.{}, true);
        try std.testing.expectEqual(@as(usize, 1), own.namespace.state.distinct.len);
        try std.testing.expect(memory.reads < 40 and memory.scans < 16);
        std.debug.print("ROOT DIRECTED edits={d} gets={d} seeks={d} own_pairs=1\n", .{ count, memory.reads, memory.scans });
        _ = try store.queryPrepared(prepared, &.{"entity/curated-root"}, &.{}, true);
        memory.reads = 0;
        memory.scans = 0;
        _ = try store.queryPrepared(prepared, &.{"entity/curated-root"}, &.{}, true);
        try std.testing.expect(memory.reads < 40 and memory.scans < 20);
        std.debug.print("WARM ROUTING edits={d} gets={d} seeks={d}\n", .{ count, memory.reads, memory.scans });
        const routing_before = prepared.routing_id;
        const saved = (try store.backend.get(store.backend.ptr, a, try store.routingPath(template))).?;
        var next = namespace;
        next.revision += 1;
        next.state.distinct = &.{.{ .from = aliases[0].key, .to = "entity/new-pin" }};
        try store.apply(try difference(a, namespace, next));
        namespace = next;
        const pinned = (try store.prepareQuery(template, profile, true)).?;
        defer pinned.release();
        try std.testing.expectEqualSlices(u8, &routing_before, &pinned.routing_id);
        try std.testing.expectEqual(@as(usize, 1), (try store.queryPrepared(pinned, &.{"entity/curated-root"}, &.{}, true)).namespace.state.distinct.len);
        next = namespace;
        next.revision += 1;
        const changed = try a.dupe(resolver.KeyAlias, natives);
        changed[0].target = "entity/moved-root";
        next.merges = changed;
        try store.apply(try difference(a, namespace, next));
        const moved = (try store.prepareQuery(template, profile, true)).?;
        defer moved.release();
        try std.testing.expect(!std.mem.eql(u8, &routing_before, &moved.routing_id));
        try std.testing.expectEqual(@as(usize, 0), (try store.queryPrepared(moved, &.{"entity/curated-root"}, &.{}, true)).namespace.state.distinct.len);
        try std.testing.expectEqual(@as(usize, 1), (try store.queryPrepared(moved, &.{"entity/moved-root"}, &.{}, true)).namespace.state.distinct.len);
        try store.backend.put(store.backend.ptr, a, try store.routingPath(template), saved);
        const stale = (try store.prepareQuery(template, profile, true)).?;
        defer stale.release();
        try std.testing.expectEqualSlices(u8, &stale.stamp.fingerprint, &stale.routing_id);
        try std.testing.expectEqual(@as(usize, 1), (try store.queryPrepared(stale, &.{"entity/moved-root"}, &.{}, true)).namespace.state.distinct.len);
    }
}

test "native resolver authority indexed admission avoids profile loads under leased base pressure" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Pressure = struct {
        fn load(_: *anyopaque, alloc: A) !query_context.Graph {
            const bytes = try alloc.alloc(u8, 28 * 1024 * 1024);
            @memset(bytes, 'x');
            return .{ .shared_profile = true, .routes = try alloc.dupe(query_context.Route, &.{.{ .root = bytes, .component = "pressure" }}) };
        }
        fn noLoad(ptr: *anyopaque, _: A) ![]const resolver.KeyAlias {
            const calls: *usize = @ptrCast(@alignCast(ptr));
            calls.* += 1;
            return error.OutOfMemory;
        }
    };
    var calls: usize = 0;
    const pressure = try query_context.acquire(@splat(210), true, .{ .ptr = &calls, .load = Pressure.load });
    defer pressure.release();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "base-admission/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    try store.adoptNamespace(.{ .key_template = template, .revision = 1, .state = .{ .distinct = &.{.{ .from = "entity/pin", .to = "entity/other" }} } });
    var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "unused-projection/", .profile = "unloaded", .indexed = true };
    for (0..3) |_| {
        const prepared = (try store.prepareQuery(template, .{ .id = "admission-unloaded", .ptr = &calls, .load = Pressure.noLoad, .estimated_bytes = 8 * 1024 * 1024, .lookup = reader.lookup() }, true)).?;
        defer prepared.release();
        try std.testing.expect(prepared.indexed and prepared.lease == null);
        try std.testing.expectEqual(@as(usize, 1), (try store.queryPrepared(prepared, &.{"entity/pin"}, &.{}, true)).namespace.state.distinct.len);
    }
    try std.testing.expectEqual(@as(usize, 0), calls);
    std.debug.print("BASE ADMISSION pages=3 profile_loads=0 leased_bytes={d}\n", .{pressure.context.arena.queryCapacity()});
}

test "native resolver authority effective root pin membership skips dense split components and fences edits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]usize{ 100, 1000 }) |count| {
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = try std.fmt.allocPrint(a, "dense-split-{d}/", .{count}), .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const template = "entity/{{_key}}";
        const aliases = try a.alloc(resolver.KeyAlias, count);
        const pairs = try a.alloc(c.Pair, count);
        for (aliases, pairs, 0..) |*alias, *pair, i| {
            alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/pin-branch-{d}", .{i}), .target = "entity/static-root" };
            pair.* = .{ .from = alias.key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
        }
        var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{.{ .key = aliases[0].key, .target = "entity/moved-root" }}, .state = .{ .distinct = pairs } };
        try store.adoptNamespace(namespace);
        var inline_profile: Store.InlineQueryProfile = .{ .aliases = aliases };
        const profile: Store.QueryProfile = .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a), .ptr = &inline_profile, .load = Store.InlineQueryProfile.load, .estimated_bytes = std.math.maxInt(usize) };
        const prepared = (try store.prepareQuery(template, profile, true)).?;
        defer prepared.release();
        _ = try store.queryPrepared(prepared, &.{"entity/moved-root"}, &.{}, true);
        memory.reads = 0;
        memory.scans = 0;
        const warm = try store.queryPrepared(prepared, &.{"entity/moved-root"}, &.{}, true);
        try std.testing.expectEqual(@as(usize, 1), warm.namespace.state.distinct.len);
        try std.testing.expect(memory.reads < 64 and memory.scans < 16);
        std.debug.print("DENSE PIN MEMBERSHIP pins={d} gets={d} seeks={d} pairs=1\n", .{ count, memory.reads, memory.scans });
        // A denied cache admission must not retain discarded pin postings.
        {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var current = store;
            current.a = request.allocator();
            var uncached = prepared;
            uncached.profile_id = "";
            const streamed = try current.queryPrepared(uncached, &.{"entity/moved-root"}, &.{}, true);
            try std.testing.expectEqual(@as(usize, 1), streamed.namespace.state.distinct.len);
            try std.testing.expect(request.queryCapacity() < 128 * 1024);
        }
        var next = namespace;
        next.revision += 1;
        next.state.suppress.texts = &.{"unrelated text"};
        try store.apply(try difference(a, namespace, next));
        namespace = next;
        const text_edit = (try store.prepareQuery(template, profile, true)).?;
        defer text_edit.release();
        try std.testing.expectEqualSlices(u8, &prepared.graph_id, &text_edit.graph_id);
        memory.reads = 0;
        _ = try store.queryPrepared(text_edit, &.{"entity/moved-root"}, &.{}, true);
        try std.testing.expect(memory.reads < 64);
        next = namespace;
        next.revision += 1;
        next.state.distinct = pairs[1..];
        try store.apply(try difference(a, namespace, next));
        namespace = next;
        const removed = (try store.prepareQuery(template, profile, true)).?;
        defer removed.release();
        try std.testing.expect(!std.mem.eql(u8, &prepared.graph_id, &removed.graph_id));
        try std.testing.expectEqualSlices(u8, &prepared.routing_id, &removed.routing_id);
        try std.testing.expectEqual(@as(usize, 0), (try store.queryPrepared(removed, &.{"entity/moved-root"}, &.{}, true)).namespace.state.distinct.len);
        next = namespace;
        next.revision += 1;
        next.merges = &.{.{ .key = aliases[1].key, .target = "entity/moved-root" }};
        try store.apply(try difference(a, namespace, next));
        const redirected = (try store.prepareQuery(template, profile, true)).?;
        defer redirected.release();
        try std.testing.expectEqual(@as(usize, 1), (try store.queryPrepared(redirected, &.{"entity/moved-root"}, &.{}, true)).namespace.state.distinct.len);
        try std.testing.expectError(error.ResolverCurationConflict, store.queryPrepared(prepared, &.{"entity/moved-root"}, &.{}, true));
    }
}

test "resolver curation query execution projection preserves gates through cached and indexed paths" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "execution-policy/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    const aliases: []const resolver.KeyAlias = &.{ .{ .key = "entity/a", .target = "entity/b" }, .{ .key = "entity/b", .target = "entity/root" }, .{ .key = "entity/removed", .target = "entity/root" }, .{ .key = "entity/pinned", .target = "entity/root" } };
    const namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{.{ .key = "entity/b", .target = "entity/native" }}, .state = .{ .distinct = &.{.{ .from = "entity/pinned", .to = "entity/other" }}, .removed_aliases = &.{"entity/removed"}, .suppress = .{ .keys = &.{"entity/native"}, .texts = &.{"noise"} } } };
    try store.apply(try difference(a, .{ .key_template = template }, namespace));
    var loader: Store.InlineQueryProfile = .{ .aliases = aliases };
    const id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a);
    for ([_]usize{ loader.estimate(), std.math.maxInt(usize) }) |estimate| {
        const prepared = (try store.prepareQuery(template, .{ .id = id, .ptr = &loader, .load = Store.InlineQueryProfile.load, .estimated_bytes = estimate }, true)).?;
        defer prepared.release();
        const seeds: []const []const u8 = &.{ "entity/a", "entity/removed", "entity/root" };
        const part = try store.queryExecutionPrepared(prepared, seeds, &.{"Noise"}, true);
        try std.testing.expect(part.complete_aliases);
        const joined = try unionQueryPolicies(a, &.{ part, part });
        var projected = try resolver.Resolver.initFromParts(a, "entities", template, .{}, false, "");
        defer projected.deinit();
        try projected.installQueryProjection(a, id, "{\"keys\":[\"entity/configured-negative\"]}", joined.namespace.merges, try encode(a, joined.namespace.state), seeds);
        try std.testing.expectEqualStrings("entity/native", projected.alias_index.?.canonical("entity/a"));
        try std.testing.expectEqualStrings("entity/removed", projected.alias_index.?.canonical("entity/removed"));
        try std.testing.expect(projected.curation_index.keySuppressed("entity/native"));
        try std.testing.expect(projected.curation_index.keySuppressed("entity/configured-negative"));
        try std.testing.expect(try projected.curation_index.pinnedDistinct(a, "entity/root", "entity/other"));
        try std.testing.expectEqual(c.Reason.text, (try projected.curation_index.textReason(a, "Noise")).?);
        try std.testing.expectEqual(@as(usize, 0), projected.query_policy_lease.?.aliases().len);
        var legacy = part;
        legacy.complete_aliases = false;
        try std.testing.expectError(error.ResolverCurationConflict, unionQueryPolicies(a, &.{ part, legacy }));
    }
}

test "resolver curation query execution uses ready nodes without loading oversized profiles" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "oversized-execution/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    try store.adoptNamespace(.{ .key_template = template, .revision = 1 });
    const Ready = struct {
        calls: usize = 0,
        fn load(_: *anyopaque, _: A) ![]const resolver.KeyAlias {
            return error.UnexpectedFullProfileRead;
        }
        fn expand(ptr: *anyopaque, alloc: A, id: []const u8, forward_only: bool, _: EdgeMask) ![]const resolver.KeyAlias {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expect(forward_only);
            return if (std.mem.eql(u8, id, "entity/a")) alloc.dupe(resolver.KeyAlias, &.{.{ .key = "entity/a", .target = "entity/root" }}) else &.{};
        }
        fn seek(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8) !?query_context.QueryNode {
            return error.UnexpectedPinDiscovery;
        }
    };
    var ready: Ready = .{};
    const prepared = (try store.prepareQuery(template, .{ .id = "oversized-profile", .ptr = &ready, .load = Ready.load, .estimated_bytes = 100000 * 4096, .lookup = .{ .ptr = &ready, .expand = Ready.expand, .seek_node = Ready.seek } }, false)).?;
    defer prepared.release();
    try std.testing.expect(prepared.indexed and prepared.lease == null);
    for (0..10) |_| {
        var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer scratch.deinit();
        var page_store = store;
        page_store.a = scratch.allocator();
        const snapshot = try page_store.queryExecutionPrepared(prepared, &.{"entity/a"}, &.{}, false);
        var target = try resolver.Resolver.initFromParts(scratch.allocator(), "entities", template, .{}, false, "");
        defer target.deinit();
        try target.installQueryProjection(scratch.allocator(), prepared.profile_id, "", snapshot.namespace.merges, try encode(scratch.allocator(), snapshot.namespace.state), &.{"entity/a"});
        try std.testing.expectEqualStrings("entity/root", target.alias_index.?.canonical("entity/a"));
        try std.testing.expectEqual(@as(usize, 0), target.query_policy_lease.?.aliases().len);
        try std.testing.expect(scratch.queryCapacity() < 32 * 1024);
    }
    try std.testing.expectEqual(@as(usize, 20), ready.calls);
    std.debug.print("OVERSIZED PROJECTION phases=10 full_profile_loads=0 point_expansions=20\n", .{});
}

test "resolver curation ready query indexes bound alternating profiles independently of graph cache residency" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: TestMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "working-set/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
    const template = "entity/{{_key}}";
    try store.adoptNamespace(.{ .key_template = template, .revision = 1 });
    const Txn = struct {
        memory: *TestMemory,
        pub fn get(self: *@This(), key_: []const u8) ![]const u8 {
            return self.memory.rows.get(key_) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key_: []const u8, raw: []const u8) !void {
            return TestMemory.put(self.memory, self.memory.a, key_, raw);
        }
        pub fn delete(self: *@This(), key_: []const u8) !void {
            return TestMemory.remove(self.memory, self.memory.a, key_);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    const Profile = struct {
        id: []const u8,
        aliases: []const resolver.KeyAlias,
        loads: usize = 0,
        fn load(ptr: *anyopaque, _: A) ![]const resolver.KeyAlias {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.loads += 1;
            return error.UnexpectedFullProfileLoad;
        }
    };
    var profiles: [5]Profile = undefined;
    var readers: [5]static_profiles.QueryReader = undefined;
    for (&profiles, &readers, 0..) |*profile, *reader, i| {
        const aliases = try a.alloc(resolver.KeyAlias, 5000);
        for (aliases, 0..) |*edge, j| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/working-{d}-{d}", .{ i, j }), .target = try std.fmt.allocPrint(a, "entity/working-root-{d}", .{i}) };
        const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
        profile.* = .{ .id = try definition.identity(a), .aliases = aliases };
        try static_profiles.synchronize(a, &txn, "working-profiles/", &.{}, &.{definition});
        reader.* = .{ .backend = store.backend, .root = "working-profiles/", .profile = profile.id, .indexed = try static_profiles.queryNodesReady(a, &txn, "working-profiles/", profile.id) };
    }
    const builds = test_query_base_loads.load(.monotonic);
    var max_arena: usize = 0;
    for (0..3) |phase| for (&profiles, &readers) |*profile, *reader| {
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var current = store;
        current.a = request.allocator();
        const inline_profile: Store.InlineQueryProfile = .{ .aliases = profile.aliases };
        const input: Store.QueryProfile = if (phase == 2)
            .{ .id = profile.id, .lookup = reader.lookup() }
        else
            .{ .id = profile.id, .ptr = profile, .load = Profile.load, .estimated_bytes = if (phase == 0) 0 else inline_profile.estimate(), .lookup = reader.lookup() };
        const prepared = (try current.prepareQuery(template, input, false)).?;
        defer prepared.release();
        try std.testing.expect(prepared.indexed and prepared.lease == null);
        const visited = memory.visited;
        const scans = memory.scans;
        const snapshot = try current.queryExecutionPrepared(prepared, &.{ profile.aliases[0].key, "entity/absent" }, &.{}, false);
        try std.testing.expectEqual(@as(usize, 1), snapshot.namespace.merges.len);
        try std.testing.expectEqualStrings(profile.aliases[0].target, snapshot.namespace.merges[0].target);
        try std.testing.expect(memory.visited - visited <= 2);
        try std.testing.expect(memory.scans - scans <= 4);
        max_arena = @max(max_arena, request.queryCapacity());
        try std.testing.expect(request.queryCapacity() < 128 * 1024);
        try std.testing.expectEqual(@as(usize, 0), profile.loads);
    };
    try std.testing.expectEqual(builds, test_query_base_loads.load(.monotonic));
    // Current native head is still required even when no graph is compiled.
    const prior = (try store.exportNamespace(template)).?.namespace;
    const stale = (try store.prepareQuery(template, .{ .id = profiles[0].id, .lookup = readers[0].lookup() }, false)).?;
    defer stale.release();
    var next = prior;
    next.revision += 1;
    next.merges = &.{.{ .key = profiles[0].aliases[0].key, .target = "entity/moved" }};
    try store.apply(try difference(a, prior, next));
    try std.testing.expectError(error.ResolverCurationConflict, store.queryExecutionPrepared(stale, &.{profiles[0].aliases[0].key}, &.{}, false));
    const fresh = (try store.prepareQuery(template, .{ .id = profiles[0].id, .lookup = readers[0].lookup() }, false)).?;
    defer fresh.release();
    const moved = try store.queryExecutionPrepared(fresh, &.{profiles[0].aliases[0].key}, &.{}, false);
    try std.testing.expectEqualStrings("entity/moved", moved.namespace.merges[0].target);
    try std.testing.expectEqual(builds, test_query_base_loads.load(.monotonic));
    std.debug.print("NATIVE QUERY WORKING SET profiles=5 aliases_per_profile=5000 passes=3 full_profile_loads=0 graph_leases=0 max_request_arena={d} visited_rows_per_query<=2 head_changes=fenced\n", .{max_arena});
}

test "resolver curation pin join excludes ten thousand unrelated overrides cold warm and without cache" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = "pin-join-large/", .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const aliases = try a.alloc(resolver.KeyAlias, 10000);
    const natives = try a.alloc(resolver.KeyAlias, aliases.len);
    for (aliases, natives, 0..) |*alias, *edge, i| {
        alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/join-{d}", .{i}), .target = "entity/static-root" };
        edge.* = .{ .key = alias.key, .target = "entity/curated-root" };
    }
    var namespace: policy.Namespace = .{ .key_template = "entity/{{_key}}", .revision = 1, .merges = natives, .state = .{ .distinct = &.{.{ .from = aliases[0].key, .to = "entity/other" }} } };
    try store.adoptNamespace(namespace);
    const Ready = struct {
        nodes: query_context.NodeIndex,
        fn node(ptr: *anyopaque, _: A, id: []const u8) !?query_context.QueryNode {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.nodes.by_key.get(id);
        }
        fn seek(ptr: *anyopaque, _: A, digest: []const u8, scope: ?[]const u8) !?query_context.QueryNode {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.nodes.seek(digest, scope);
        }
        fn expand(_: *anyopaque, _: A, id: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
            // Every configured edge is masked by a native override.
            if (std.mem.startsWith(u8, id, "entity/join-")) return error.UnexpectedConfiguredExpansion;
            return &.{};
        }
    };
    var ready: Ready = .{ .nodes = try query_context.NodeIndex.init(a, aliases) };
    const profile: Store.QueryProfile = .{ .id = try (static_profiles.Definition{ .key_template = namespace.key_template, .aliases = aliases }).identity(a), .lookup = .{ .ptr = &ready, .node = Ready.node, .seek_node = Ready.seek, .expand = Ready.expand } };
    var max_reads: usize = 0;
    var max_scans: usize = 0;
    var max_arena: usize = 0;
    for (0..4) |phase| {
        if (phase == 3) {
            var next = namespace;
            next.revision += 1;
            const moved = try a.dupe(resolver.KeyAlias, namespace.merges);
            moved[1].target = "entity/unrelated-root";
            next.merges = moved;
            try store.apply(try difference(a, namespace, next));
            namespace = next;
        }
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var current = store;
        current.a = request.allocator();
        var prepared = (try current.prepareQuery(namespace.key_template, profile, true)).?;
        defer prepared.release();
        if (phase == 2) prepared.profile_id = ""; // The exact cache-admission fallback.
        memory.reads = 0;
        memory.scans = 0;
        const result = try current.queryExecutionPrepared(prepared, &.{"entity/curated-root"}, &.{}, true);
        try std.testing.expectEqual(@as(usize, 1), result.namespace.state.distinct.len);
        try std.testing.expectEqualStrings(aliases[0].key, result.namespace.state.distinct[0].from);
        max_reads = @max(max_reads, memory.reads);
        max_scans = @max(max_scans, memory.scans);
        max_arena = @max(max_arena, request.queryCapacity());
        try std.testing.expect(memory.reads < 64 and memory.scans < 20);
        try std.testing.expect(request.queryCapacity() < 128 * 1024);
    }
    std.debug.print("SPARSE PIN JOIN overrides=10000 phases=4 max_gets={d} max_scans={d} max_request_arena={d} pairs=1\n", .{ max_reads, max_scans, max_arena });
}

test "resolver curation sparse pin join matches full composition through alternating native and configured paths" {
    var rng = std.Random.DefaultPrng.init(718235);
    const random = rng.random();
    for (0..32) |trial| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: TestMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = "pin-join-reference/", .backend = .{ .ptr = &memory, .get = TestMemory.get, .put = TestMemory.put, .remove = TestMemory.remove, .scan = TestMemory.scan } };
        var names: [32][]const u8 = undefined;
        for (&names, 0..) |*name, i| name.* = try std.fmt.allocPrint(a, "entity/trial-{d}-{d}", .{ trial, i });
        var aliases: [24]resolver.KeyAlias = undefined;
        var natives: std.ArrayList(resolver.KeyAlias) = .empty;
        var removed: std.ArrayList([]const u8) = .empty;
        var effective: std.ArrayList(resolver.KeyAlias) = .empty;
        for (&aliases, 0..) |*alias, i| {
            alias.* = .{ .key = names[i], .target = names[random.intRangeAtMost(usize, i + 1, 31)] };
            switch (random.uintLessThan(u8, 4)) {
                0 => {
                    const edge: resolver.KeyAlias = .{ .key = names[i], .target = names[random.intRangeAtMost(usize, i + 1, 31)] };
                    try natives.append(a, edge);
                    try effective.append(a, edge);
                },
                1 => try removed.append(a, names[i]),
                else => try effective.append(a, alias.*),
            }
        }
        var pairs: [6]c.Pair = undefined;
        for (&pairs, 0..) |*pair, i| pair.* = .{ .from = names[i], .to = names[i + 24] };
        const namespace: policy.Namespace = .{ .key_template = "entity/{{_key}}", .revision = 1, .merges = natives.items, .state = .{ .distinct = &pairs, .removed_aliases = removed.items } };
        try store.adoptNamespace(namespace);
        var inline_profile: Store.InlineQueryProfile = .{ .aliases = &aliases };
        const prepared = (try store.prepareQuery(namespace.key_template, .{ .id = try (static_profiles.Definition{ .key_template = namespace.key_template, .aliases = &aliases }).identity(a), .ptr = &inline_profile, .load = Store.InlineQueryProfile.load, .estimated_bytes = std.math.maxInt(usize) }, true)).?;
        defer prepared.release();
        if (trial % 2 == 1) {
            var local: Store.StaticQueryLookup = .{ .context = prepared.lease.?.context };
            const lookup: AliasLookup = .{ .ptr = &local, .expand = Store.StaticQueryLookup.expand, .node = Store.StaticQueryLookup.node, .seek_node = Store.StaticQueryLookup.seekNode };
            var turns: usize = 0;
            while (!try store.advanceQueryPins(namespace.key_template, prepared.profile_id, lookup, 32)) : (turns += 1) try std.testing.expect(turns < 64);
        }
        const reference = try c.RedirectIndex.init(a, effective.items);
        for (names) |seed| {
            var expected: std.StringHashMapUnmanaged(void) = .empty;
            for (pairs) |pair| if (std.mem.eql(u8, reference.canonical(seed), reference.canonical(pair.from)) or std.mem.eql(u8, reference.canonical(seed), reference.canonical(pair.to))) try expected.put(a, try c.pairKey(a, pair.from, pair.to), {});
            // Exercise the exact uncached path as well as the immutable projection.
            var input = prepared;
            if (trial % 2 == 0) input.profile_id = "";
            const result = try store.queryExecutionPrepared(input, &.{seed}, &.{}, true);
            try std.testing.expectEqual(expected.count(), result.namespace.state.distinct.len);
            for (result.namespace.state.distinct) |pair| try std.testing.expect(expected.contains(try c.pairKey(a, pair.from, pair.to)));
        }
    }
}

// Ordered fixture cursor: mutation and page costs stay logarithmic while the
// regression counts every native/profile storage call and retained row.
const OrderedPinMemory = struct {
    const Tree = std.Treap([]const u8, struct {
        fn order(left: []const u8, right: []const u8) std.math.Order {
            return std.mem.order(u8, left, right);
        }
    }.order);
    a: A,
    rows: std.StringHashMapUnmanaged([]const u8) = .empty,
    tree: Tree = .{},
    reads: usize = 0,
    pin_event_reads: usize = 0,
    scans: usize = 0,
    writes: usize = 0,
    visited: usize = 0,
    distinct_rows: usize = 0,
    fn get(ptr: *anyopaque, a: A, key: []const u8) !?[]const u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.reads += 1;
        if (std.mem.indexOf(u8, key, "pin-edits-v1/") != null and std.mem.indexOf(u8, key, "/slot/") != null) self.pin_event_reads += 1;
        return if (self.rows.get(key)) |value| try a.dupe(u8, value) else null;
    }
    fn put(ptr: *anyopaque, _: A, key: []const u8, value: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        const entry = try self.rows.getOrPut(self.a, key);
        if (!entry.found_existing) entry.key_ptr.* = try self.a.dupe(u8, key);
        entry.value_ptr.* = try self.a.dupe(u8, value);
        var ordered = self.tree.getEntryFor(entry.key_ptr.*);
        if (ordered.node == null) ordered.set(try self.a.create(Tree.Node));
    }
    fn remove(ptr: *anyopaque, _: A, key: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        var entry = self.tree.getEntryFor(key);
        entry.set(null);
        _ = self.rows.remove(key);
    }
    fn scan(ptr: *anyopaque, a: A, prefix: []const u8, after: ?[]const u8, limit: u32) !Page {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.scans += 1;
        const lower = after orelse prefix;
        var current = self.tree.root;
        var first: ?*Tree.Node = null;
        while (current) |node| {
            const order = std.mem.order(u8, node.key, lower);
            if (order == .lt or (after != null and order == .eq)) current = node.children[1] else {
                first = node;
                current = node.children[0];
            }
        }
        var rows: std.ArrayList(Row) = .empty;
        var item = first;
        while (item) |node| : (item = node.next()) {
            if (!std.mem.startsWith(u8, node.key, prefix)) break;
            if (rows.items.len == limit) return .{ .rows = rows.items, .next = rows.items[rows.items.len - 1].key };
            try rows.append(a, .{ .key = try a.dupe(u8, node.key), .value = try a.dupe(u8, self.rows.get(node.key).?) });
            self.visited += 1;
            if (std.mem.endsWith(u8, prefix, "/distinct/")) self.distinct_rows += 1;
        }
        return .{ .rows = rows.items };
    }
};

test "resolver curation root pin projections bound six thousand independent components and fence owner generations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const aliases = try a.alloc(resolver.KeyAlias, 6000);
    const pairs = try a.alloc(c.Pair, aliases.len);
    const pad = try a.alloc(u8, 128);
    @memset(pad, 'x');
    for (aliases, pairs, 0..) |*alias, *pair, i| {
        alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/pin-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/root-{d}-{s}", .{ i, pad }) };
        pair.* = .{ .from = alias.key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
    }
    const template = "entity/{{_key}}";
    var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs } };
    try store.adoptNamespace(namespace);
    const Txn = struct {
        memory: *OrderedPinMemory,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.memory.rows.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            return OrderedPinMemory.put(self.memory, self.memory.a, key, value);
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            return OrderedPinMemory.remove(self.memory, self.memory.a, key);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
    const id = try definition.identity(a);
    try static_profiles.synchronize(a, &txn, "pin-profiles/", &.{}, &.{definition});
    var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "pin-profiles/", .profile = id, .indexed = true };
    const profile: Store.QueryProfile = .{ .id = id, .lookup = reader.lookup() };
    var max_scans: usize = 0;
    var max_arena: usize = 0;
    var total_turns: usize = 0;
    var bootstrap_turns: usize = 0;
    while (!try store.advanceQueryPins(template, id, reader.lookup(), 128)) : (bootstrap_turns += 1) try std.testing.expect(bootstrap_turns < 2048);
    for (0..4) |phase| {
        if (phase != 0) {
            var next = namespace;
            next.revision += 1;
            next.merges = if (phase == 1) &.{.{ .key = "entity/unrelated", .target = "entity/unrelated-root" }} else if (phase == 2) &.{ .{ .key = "entity/unrelated", .target = "entity/unrelated-root" }, .{ .key = aliases[0].key, .target = "entity/moved" } } else &.{};
            try store.apply(try difference(a, namespace, next));
            namespace = next;
            if (phase != 3) {
                try std.testing.expect(!try store.queryPinsIndexed(template, id));
                const prepared = (try store.prepareQuery(template, profile, true)).?;
                defer prepared.release();
                var roots: std.StringHashMapUnmanaged(void) = .empty;
                try roots.put(a, aliases[0].target, {});
                var starts: std.StringHashMapUnmanaged(void) = .empty;
                try std.testing.expectEqual(phase == 1, try store.projectedQueryPins(prepared, roots, reader.lookup(), &starts));
                var turns: usize = 0;
                while (true) : (turns += 1) {
                    var request = std.heap.ArenaAllocator.init(std.testing.allocator);
                    defer request.deinit();
                    var owner = store;
                    owner.a = request.allocator();
                    const writes = memory.writes;
                    const complete = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
                    try std.testing.expect(memory.writes - writes <= 129);
                    try std.testing.expect(turns < 1024);
                    total_turns += 1;
                    if (complete) break;
                }
                try std.testing.expect(try store.queryPinsIndexed(template, id));
                const projection_root = try store.pinProjectionRoot(template, id);
                const state_path = try std.mem.concat(a, u8, &.{ projection_root, "state" });
                try validatePortableRow(a, state_path, memory.rows.get(state_path).?);
                const canonical_root = if (phase == 2) "entity/moved" else aliases[0].target;
                const member_path = try std.fmt.allocPrint(a, "{s}member/{s}/{s}", .{ projection_root, try hash(a, canonical_root), try hash(a, aliases[0].key) });
                try validatePortableRow(a, member_path, memory.rows.get(member_path).?);
            }
        }
        for (0..3) |_| {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var current = store;
            current.a = request.allocator();
            const prepared = (try current.prepareQuery(template, profile, true)).?;
            defer prepared.release();
            const scans = memory.scans;
            const seed = if (phase == 2) "entity/moved" else aliases[0].target;
            const result = try current.queryExecutionPrepared(prepared, &.{seed}, &.{}, true);
            try std.testing.expectEqual(@as(usize, 1), result.namespace.state.distinct.len);
            try std.testing.expect(memory.scans - scans < 24);
            try std.testing.expect(request.queryCapacity() < 128 * 1024);
            max_scans = @max(max_scans, memory.scans - scans);
            max_arena = @max(max_arena, request.queryCapacity());
            if (phase == 2) try std.testing.expectEqual(@as(usize, 0), (try current.queryExecutionPrepared(prepared, &.{aliases[0].target}, &.{}, true)).namespace.state.distinct.len);
        }
    }
    const replacement: static_profiles.Definition = .{ .key_template = template, .aliases = aliases, .suppress_json = "{\"keys\":[\"entity/retired-profile-negative\"]}" };
    try static_profiles.synchronize(a, &txn, "pin-profiles/", &.{definition}, &.{replacement});
    var replacement_reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "pin-profiles/", .profile = try replacement.identity(a), .indexed = true };
    var gc_turns: usize = 0;
    while (true) : (gc_turns += 1) {
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var owner = store;
        owner.a = request.allocator();
        const writes = memory.writes;
        const done = try owner.advanceQueryPins(template, replacement_reader.profile, replacement_reader.lookup(), 128);
        try std.testing.expect(memory.writes - writes <= 129);
        try std.testing.expect(gc_turns < 4096);
        if (done) break;
    }
    const retired_prefix = try store.pinProjectionRoot(template, id);
    try std.testing.expectEqual(@as(usize, 0), (try store.backend.scan(store.backend.ptr, a, retired_prefix, null, 1)).rows.len);
    try std.testing.expect(try store.queryPinsCleanupCurrent(template, replacement_reader.lookup()));
    var rows = memory.rows.iterator();
    while (rows.next()) |row| if (std.mem.startsWith(u8, row.key_ptr.*, lite_prefix ++ "query-pins-v1/") or std.mem.startsWith(u8, row.key_ptr.*, lite_prefix ++ "query-pins-gc-v1/")) try validatePortableRow(a, row.key_ptr.*, row.value_ptr.*);
    std.debug.print("ROOT PIN PROJECTION components=6000 phases=4 reads_per_phase=3 max_scans={d} max_request_arena={d} owner_turns={d} writes_per_turn<=129 generations=fenced\n", .{ max_scans, max_arena, total_turns });
}

test "resolver curation static root membership excludes dense unrelated pins on repeated reads" {
    for ([_]usize{ 1000, 6000 }) |count| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const aliases = try a.alloc(resolver.KeyAlias, count * 2);
        const pairs = try a.alloc(c.Pair, count);
        for (pairs, 0..) |*pair, i| {
            aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
            aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = "entity/unpinned-root" };
            pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
        }
        const template = "entity/{{_key}}";
        const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs } };
        try store.adoptNamespace(initial);
        const Txn = struct {
            memory: *OrderedPinMemory,
            pub fn get(self: *@This(), k: []const u8) ![]const u8 {
                return self.memory.rows.get(k) orelse error.NotFound;
            }
            pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
                return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
            }
            pub fn delete(self: *@This(), k: []const u8) !void {
                return OrderedPinMemory.remove(self.memory, self.memory.a, k);
            }
        };
        var txn: Txn = .{ .memory = &memory };
        const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
        const id = try definition.identity(a);
        try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
        var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
        var turns: usize = 0;
        while (true) : (turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const writes = memory.writes;
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - writes <= 129);
            try std.testing.expect(turns < (pairs.len * 32 + 111) / 112 + 32);
            if (done) break;
        }
        var max_scans: usize = 0;
        for (0..3) |_| {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var current = store;
            current.a = request.allocator();
            const prepared = (try current.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
            defer prepared.release();
            const before = memory.scans;
            const result = try current.queryExecutionPrepared(prepared, &.{"entity/unpinned-root"}, &.{}, true);
            max_scans = @max(max_scans, memory.scans - before);
            try std.testing.expect(memory.scans - before <= 12);
            try std.testing.expect(request.queryCapacity() < 128 * 1024);
            try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
        }
        // A new native pin immediately invalidates static readiness, too.
        var next = initial;
        next.revision += 1;
        const extended = try a.alloc(c.Pair, pairs.len + 1);
        @memcpy(extended[0..pairs.len], pairs);
        extended[pairs.len] = .{ .from = "entity/unpinned-root", .to = "entity/new-other" };
        next.state.distinct = extended;
        try store.apply(try difference(a, initial, next));
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
        const before_edit_writes = memory.writes;
        const before_edit_scans = memory.scans;
        var edit_turns: usize = 0;
        while (true) : (edit_turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(edit_turns < 3);
            if (done) break;
        }
        try std.testing.expect(memory.writes - before_edit_writes <= 32);
        try std.testing.expect(memory.scans - before_edit_scans <= 32);
        std.debug.print("INCREMENTAL PIN unrelated_pins={d} owner_turns={d} writes={d} scans={d}\n", .{ count, edit_turns + 1, memory.writes - before_edit_writes, memory.scans - before_edit_scans });
        const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer prepared.release();
        try std.testing.expectEqual(@as(usize, 1), (try store.queryExecutionPrepared(prepared, &.{"entity/unpinned-root"}, &.{}, true)).namespace.state.distinct.len);
        var reverted = next;
        reverted.revision += 1;
        reverted.state.distinct = pairs;
        try store.apply(try difference(a, next, reverted));
        const before_remove = memory.writes;
        try std.testing.expect(try store.advanceQueryPins(template, id, reader.lookup(), 128));
        try std.testing.expect(memory.writes - before_remove <= 32);
        const removed = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer removed.release();
        try std.testing.expectEqual(@as(usize, 0), (try store.queryExecutionPrepared(removed, &.{"entity/unpinned-root"}, &.{}, true)).namespace.state.distinct.len);
        std.debug.print("STATIC ROOT MEMBERSHIP unrelated_pins={d} repeated_reads=3 max_scans={d} owner_turns={d} invalidation=fenced\n", .{ count, max_scans, turns + 1 });
    }
}

test "resolver curation owner resumes and compresses sixteen thousand configured hops with bounded work" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const template = "entity/{{_key}}";
    const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{.{ .key = "entity/unrelated", .target = "entity/elsewhere" }}, .state = .{ .distinct = &.{ .{ .from = "entity/link-0", .to = "entity/other" }, .{ .from = "entity/link-4000", .to = "entity/other" } } } };
    try store.adoptNamespace(initial);
    // Faithful indexed fixture for a long immutable configured chain. Scalar
    // nodes carry its canonical root; expand returns only the next edge.
    const Lookup = struct {
        calls: usize = 0,
        fn node(_: *anyopaque, _: A, id: []const u8) !?query_context.QueryNode {
            return if (std.mem.startsWith(u8, id, "entity/link-")) .{ .key = id, .canonical = "entity/link-16000" } else null;
        }
        fn seek(_: *anyopaque, alloc: A, digest: []const u8, _: ?[]const u8) !?query_context.QueryNode {
            const key = "entity/link-0";
            return if (std.mem.order(u8, try hash(alloc, key), digest) != .lt) .{ .key = key, .canonical = "entity/link-16000" } else null;
        }
        fn expand(ptr: *anyopaque, alloc: A, id: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (!std.mem.startsWith(u8, id, "entity/link-")) return &.{};
            const n = try std.fmt.parseInt(usize, id[12..], 10);
            if (n == 16000) return &.{};
            const edges = try alloc.alloc(resolver.KeyAlias, 1);
            edges[0] = .{ .key = id, .target = try std.fmt.allocPrint(alloc, "entity/link-{d}", .{n + 1}) };
            return edges;
        }
    };
    var fixture: Lookup = .{};
    const lookup: AliasLookup = .{ .ptr = &fixture, .node = Lookup.node, .seek_node = Lookup.seek, .expand = Lookup.expand };
    const id = try hash(a, "owner-long-configured-path");
    var turns: usize = 0;
    var max_hops: usize = 0;
    var max_arena: usize = 0;
    const state_path = try std.mem.concat(a, u8, &.{ try store.pinProjectionRoot(template, id), "state" });
    var previous = initial;
    while (true) : (turns += 1) {
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var owner = store;
        owner.a = request.allocator();
        if (turns >= 50 and turns < 550) {
            var next = previous;
            next.revision += 1;
            next.merges = &.{.{ .key = "entity/unrelated", .target = if (turns % 2 == 0) "entity/changed-a" else "entity/changed-b" }};
            try store.apply(try difference(a, previous, next));
            previous = next;
            try std.testing.expect(!try store.queryPinsIndexed(template, id));
        }
        const before = fixture.calls;
        const writes = memory.writes;
        const done = try owner.advanceQueryPins(template, id, lookup, 128);
        max_hops = @max(max_hops, fixture.calls - before);
        max_arena = @max(max_arena, request.queryCapacity());
        try std.testing.expect(fixture.calls - before <= 128);
        try std.testing.expect(memory.writes - writes <= 129);
        try std.testing.expect(request.queryCapacity() < 512 * 1024);
        if (memory.rows.get(state_path)) |raw| try validatePortableRow(a, state_path, raw) else try std.testing.expect(turns == 0);
        try std.testing.expect(turns < 3000);
        if (done) break;
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
    }
    // Five hundred unrelated heads exceed the journal retention window without
    // rewinding the persisted walk. Converging boundaries share compression.
    try std.testing.expect(fixture.calls < 40000);
    try std.testing.expect(try store.queryPinsIndexed(template, id));
    const member = try std.fmt.allocPrint(a, "{s}member/{s}/{s}", .{ try store.pinProjectionRoot(template, id), try hash(a, "entity/link-16000"), try hash(a, "entity/link-0") });
    try std.testing.expect(memory.rows.contains(member));
    // All converging boundaries can now use durable, graph-fenced shortcuts.
    const shortcut = try std.fmt.allocPrint(a, "{s}path/{s}", .{ try store.pinProjectionRoot(template, id), try hash(a, "entity/link-4000") });
    try validatePortableRow(a, shortcut, memory.rows.get(shortcut).?);
    try std.testing.expectEqualStrings("entity/link-16000", (try parse(Store.PinPath, a, memory.rows.get(shortcut).?)).root);
    const before_ready = fixture.calls;
    try std.testing.expect(try store.advanceQueryPins(template, id, lookup, 128));
    try std.testing.expectEqual(before_ready, fixture.calls);
    std.debug.print("RESUMABLE PIN PATH configured_hops=16000 owner_turns={d} max_hops_per_turn={d} max_request_arena={d} writes_per_turn<=129 durable_shortcuts=16000\n", .{ turns + 1, max_hops, max_arena });
}

test "resolver curation incremental repairs coalesce routing edits and journal gaps match full composition" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    var names: [32][]const u8 = undefined;
    for (&names, 0..) |*name, i| name.* = try std.fmt.allocPrint(a, "entity/incremental-{d}", .{i});
    var aliases: [24]resolver.KeyAlias = undefined;
    for (&aliases, 0..) |*edge, i| edge.* = .{ .key = names[i], .target = names[24 + i % 8] };
    const Reader = struct {
        nodes: query_context.NodeIndex,
        edges: []const resolver.KeyAlias,
        fn node(ptr: *anyopaque, _: A, id: []const u8) !?query_context.QueryNode {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.nodes.by_key.get(id);
        }
        fn seek(ptr: *anyopaque, _: A, digest: []const u8, root: ?[]const u8) !?query_context.QueryNode {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.nodes.seek(digest, root);
        }
        fn expand(ptr: *anyopaque, alloc: A, id: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (self.edges) |edge| if (std.mem.eql(u8, edge.key, id)) {
                const result = try alloc.alloc(resolver.KeyAlias, 1);
                result[0] = edge;
                return result;
            };
            return &.{};
        }
    };
    var reader: Reader = .{ .nodes = try query_context.NodeIndex.init(a, &aliases), .edges = &aliases };
    const lookup: AliasLookup = .{ .ptr = &reader, .node = Reader.node, .seek_node = Reader.seek, .expand = Reader.expand };
    const template = "entity/{{_key}}";
    const id = try (static_profiles.Definition{ .key_template = template, .aliases = &aliases }).identity(a);
    var pairs: [6]c.Pair = undefined;
    for (&pairs, 0..) |*pair, i| pair.* = .{ .from = names[i], .to = names[24 + i] };
    var namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = &pairs } };
    try store.adoptNamespace(namespace);
    while (!try store.advanceQueryPins(template, id, lookup, 128)) {}
    var native: [24]?resolver.KeyAlias = @splat(null);
    var removed: [24]bool = @splat(false);
    var rng = std.Random.DefaultPrng.init(98144);
    const random = rng.random();
    for (0..48) |step| {
        const slot = random.uintLessThan(usize, 24);
        native[slot] = if (step % 3 == 0) .{ .key = names[slot], .target = names[random.intRangeAtMost(usize, slot + 1, 31)] } else null;
        removed[slot] = step % 3 == 1;
        var merges: std.ArrayList(resolver.KeyAlias) = .empty;
        var removed_keys: std.ArrayList([]const u8) = .empty;
        var effective: std.ArrayList(resolver.KeyAlias) = .empty;
        for (native, removed, aliases) |edge, absent, base| {
            if (edge) |value| try merges.append(a, value) else if (absent) try removed_keys.append(a, base.key);
            if (edge) |value| try effective.append(a, value) else if (!absent) try effective.append(a, base);
        }
        var next = namespace;
        next.revision += 1;
        next.merges = merges.items;
        next.state.removed_aliases = removed_keys.items;
        try store.apply(try difference(a, namespace, next));
        namespace = next;
        _ = try store.advanceQueryPins(template, id, lookup, 32);
        // Every partial owner cut, including newly coalesced edits, must return
        // exactly the current policy. Indexed unaffected roots and fallback
        // affected roots share an independent full-composition reference.
        const reference = try c.RedirectIndex.init(a, effective.items);
        const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = lookup }, true)).?;
        defer prepared.release();
        for (names) |seed| {
            var expected: std.StringHashMapUnmanaged(void) = .empty;
            for (pairs) |pair| if (std.mem.eql(u8, reference.canonical(seed), reference.canonical(pair.from)) or std.mem.eql(u8, reference.canonical(seed), reference.canonical(pair.to))) try expected.put(a, try c.pairKey(a, pair.from, pair.to), {});
            const result = try store.queryExecutionPrepared(prepared, &.{seed}, &.{}, true);
            try std.testing.expectEqual(expected.count(), result.namespace.state.distinct.len);
            for (result.namespace.state.distinct) |pair| try std.testing.expect(expected.contains(try c.pairKey(a, pair.from, pair.to)));
        }
    }
    var turns: usize = 0;
    while (!try store.advanceQueryPins(template, id, lookup, 128)) : (turns += 1) try std.testing.expect(turns < 128);
    try std.testing.expect(try store.queryPinsIndexed(template, id));
    // The bounded journal cannot grow forever. An owner missing its retained
    // history safely rebuilds once, rather than trusting incomplete deltas.
    for (0..260) |step| {
        var next = namespace;
        next.revision += 1;
        const edges = try a.alloc(resolver.KeyAlias, namespace.merges.len + 1);
        @memcpy(edges[0..namespace.merges.len], namespace.merges);
        edges[namespace.merges.len] = .{ .key = "entity/journal-gap", .target = if (step % 2 == 0) "entity/gap-a" else "entity/gap-b" };
        // Replace the previous unrelated edge, retaining all tested policy.
        next.merges = if (step == 0) edges else blk: {
            edges[namespace.merges.len - 1] = edges[namespace.merges.len];
            break :blk edges[0..namespace.merges.len];
        };
        try store.apply(try difference(a, namespace, next));
        namespace = next;
    }
    turns = 0;
    while (!try store.advanceQueryPins(template, id, lookup, 128)) : (turns += 1) try std.testing.expect(turns < 128);
    try std.testing.expect(try store.queryPinsIndexed(template, id));
    const retained_keys = try store.backend.scan(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ try store.pinEditRoot(template), "invalidated/" }), null, 1024);
    try std.testing.expect(retained_keys.rows.len <= 3);
    const events = try store.backend.scan(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ try store.pinEditRoot(template), "slot/" }), null, 512);
    try std.testing.expectEqual(@as(usize, 256), events.rows.len);
    for (events.rows) |row| try validatePortableRow(a, row.key, row.value);
    const long_ids = try a.alloc([]const u8, 128);
    const padding = try a.alloc(u8, 1024);
    @memset(padding, 'x');
    for (long_ids, 0..) |*value, i| value.* = try std.fmt.allocPrint(a, "entity/large-{d}-{s}", .{ i, padding });
    const head = try store.pinEditHead(template, @splat(0));
    try store.appendPinEdit(template, head.graph, @splat(9), long_ids, &.{}, &.{});
    const oversized_key = try store.pinEditSlot(template, head.sequence + 1);
    const oversized_raw = memory.rows.get(oversized_key).?;
    try std.testing.expect(oversized_raw.len < 64 * 1024);
    try std.testing.expect((try parse(Store.PinEdit, a, oversized_raw)).rebuild);
    try validatePortableRow(a, oversized_key, oversized_raw);
    std.debug.print("INCREMENTAL COMPOSITION edits=48 seed_checks=1536 journal_slots=256 gap_recovery=ready\n", .{});
}

test "resolver curation dependency repair scopes leaf edits and burst validity to changed keys" {
    for ([_]usize{ 1000, 6000 }) |count| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const aliases = try a.alloc(resolver.KeyAlias, count * 2);
        const pairs = try a.alloc(c.Pair, count);
        for (pairs, 0..) |*pair, i| {
            aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
            aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = "entity/unpinned-root" };
            pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
        }
        const template = "entity/{{_key}}";
        const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs } };
        try store.adoptNamespace(initial);
        const Txn = struct {
            memory: *OrderedPinMemory,
            pub fn get(self: *@This(), k: []const u8) ![]const u8 {
                return self.memory.rows.get(k) orelse error.NotFound;
            }
            pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
                return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
            }
            pub fn delete(self: *@This(), k: []const u8) !void {
                return OrderedPinMemory.remove(self.memory, self.memory.a, k);
            }
        };
        var txn: Txn = .{ .memory = &memory };
        const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
        const id = try definition.identity(a);
        try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
        var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
        var turns: usize = 0;
        while (true) : (turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const writes = memory.writes;
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - writes <= 129);
            try std.testing.expect(turns < (pairs.len * 32 + 111) / 112 + 32);
            if (done) break;
        }
        var max_scans: usize = 0;
        for (0..3) |_| {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var current = store;
            current.a = request.allocator();
            const prepared = (try current.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
            defer prepared.release();
            const before = memory.scans;
            const result = try current.queryExecutionPrepared(prepared, &.{"entity/unpinned-root"}, &.{}, true);
            max_scans = @max(max_scans, memory.scans - before);
            try std.testing.expect(memory.scans - before <= 12);
            try std.testing.expect(request.queryCapacity() < 128 * 1024);
            try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
        }
        var next = initial;
        next.revision += 1;
        next.merges = &.{.{ .key = aliases[0].key, .target = "entity/leaf-new-root" }};
        try store.apply(try difference(a, initial, next));
        const old_writes = memory.writes;
        const old_scans = memory.scans;
        var edit_turns: usize = 0;
        while (true) : (edit_turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const before = memory.writes;
            if (try owner.advanceQueryPins(template, id, reader.lookup(), 128)) break;
            try std.testing.expect(memory.writes - before <= 129);
            try std.testing.expect(edit_turns < 4);
        }
        try std.testing.expect(memory.writes - old_writes <= 64);
        std.debug.print("DEPENDENCY LEAF pins={d} owner_turns={d} writes={d} scans={d}\n", .{ count, edit_turns + 1, memory.writes - old_writes, memory.scans - old_scans });
        var previous = next;
        for (0..145) |i| {
            var changed = previous;
            changed.revision += 1;
            const unique = i < 17;
            const edges = try a.alloc(resolver.KeyAlias, previous.merges.len + @as(usize, if (unique or i == 17) 1 else 0));
            @memcpy(edges[0..previous.merges.len], previous.merges);
            edges[edges.len - 1] = .{ .key = if (unique) try std.fmt.allocPrint(a, "entity/unrelated-edit-{d}", .{i}) else "entity/hot-edit", .target = if (i % 2 == 0) "entity/unrelated-edit-root" else "entity/unrelated-other-root" };
            changed.merges = edges;
            try store.apply(try difference(a, previous, changed));
            previous = changed;
            if (i != 16 and i != 144) continue;
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var current = store;
            current.a = request.allocator();
            const prepared = (try current.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
            defer prepared.release();
            const before_scans = memory.scans;
            const result = try current.queryExecutionPrepared(prepared, &.{"entity/unpinned-root"}, &.{}, true);
            try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
            try std.testing.expect(memory.scans - before_scans <= 12);
            try std.testing.expect(request.queryCapacity() < 256 * 1024);
            std.debug.print("DEPENDENCY BURST pins={d} unrelated_edits={d} scans={d} result_pairs={d}\n", .{ count, i + 1, memory.scans - before_scans, result.namespace.state.distinct.len });
        }
        const many_roots = try a.alloc([]const u8, 1024);
        for (many_roots, 0..) |*value, i| value.* = try std.fmt.allocPrint(a, "entity/unrelated-query-root-{d}", .{i});
        const prepared_many = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer prepared_many.release();
        const read_before = memory.reads;
        const scan_before = memory.scans;
        try std.testing.expectEqual(@as(usize, 0), (try store.queryExecutionPrepared(prepared_many, many_roots, &.{}, true)).namespace.state.distinct.len);
        try std.testing.expect(memory.reads - read_before < 14000);
        std.debug.print("DEPENDENCY MANY ROOTS roots={d} reads={d} scans={d}\n", .{ many_roots.len, memory.reads - read_before, memory.scans - scan_before });
        var proof_roots: std.StringHashMapUnmanaged(void) = .empty;
        for (many_roots) |key| try proof_roots.put(a, key, {});
        const proof_state = try parse(Store.PinProjection, a, memory.rows.get(try std.mem.concat(a, u8, &.{ try store.pinProjectionRoot(template, id), "state" })).?);
        const proof_reads = memory.reads;
        const proof_scans = memory.scans;
        try std.testing.expect(try store.pinRootsCurrent(prepared_many, proof_state, proof_roots, reader.lookup()));
        try std.testing.expect(memory.reads - proof_reads <= many_roots.len * 3 + 3);
        try std.testing.expect(memory.scans - proof_scans <= many_roots.len + 2);
        std.debug.print("DEPENDENCY PROOF ROOTS roots={d} reads={d} scans={d}\n", .{ many_roots.len, memory.reads - proof_reads, memory.scans - proof_scans });
        // The summary path must still reject real dependencies and roots,
        // even when a complete small page supplies its changed digests.
        var touched = previous;
        touched.revision += 1;
        const touched_edges = try a.alloc(resolver.KeyAlias, previous.merges.len + 1);
        @memcpy(touched_edges[0..previous.merges.len], previous.merges);
        touched_edges[touched_edges.len - 1] = .{ .key = aliases[1].key, .target = "entity/batch-new-root" };
        touched.merges = touched_edges;
        try store.apply(try difference(a, previous, touched));
        const touched_query = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer touched_query.release();
        try proof_roots.put(a, "entity/pinned-root", {});
        try std.testing.expect(!try store.pinRootsCurrent(touched_query, proof_state, proof_roots, reader.lookup()));
        _ = proof_roots.remove("entity/pinned-root");
        try proof_roots.put(a, "entity/batch-new-root", {});
        try std.testing.expect(!try store.pinRootsCurrent(touched_query, proof_state, proof_roots, reader.lookup()));
        const invalidated = try store.backend.scan(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ try store.pinEditRoot(template), "invalidated/" }), null, 256);
        try std.testing.expect(invalidated.rows.len < 32);
        var rows = memory.rows.iterator();
        while (rows.next()) |row| if (std.mem.startsWith(u8, row.key_ptr.*, lite_prefix)) try validatePortableRow(a, row.key_ptr.*, row.value_ptr.*);
    }
}

test "resolver curation dependency upgrade and masked unmerge remain exact at every owner cut" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const aliases = [_]resolver.KeyAlias{ .{ .key = "entity/x", .target = "entity/y" }, .{ .key = "entity/y", .target = "entity/c" } };
    const Reader = struct {
        nodes: query_context.NodeIndex,
        fn node(ptr: *anyopaque, _: A, id: []const u8) !?query_context.QueryNode {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.nodes.by_key.get(id);
        }
        fn seek(ptr: *anyopaque, _: A, digest: []const u8, root: ?[]const u8) !?query_context.QueryNode {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.nodes.seek(digest, root);
        }
        fn expand(_: *anyopaque, alloc: A, id: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
            for (aliases) |edge| if (std.mem.eql(u8, edge.key, id)) return alloc.dupe(resolver.KeyAlias, &.{edge});
            return &.{};
        }
    };
    var reader: Reader = .{ .nodes = try query_context.NodeIndex.init(a, &aliases) };
    const lookup: AliasLookup = .{ .ptr = &reader, .node = Reader.node, .seek_node = Reader.seek, .expand = Reader.expand };
    const id = try hash(a, "dependency-upgrade-masked-unmerge");
    const template = "entity/{{_key}}";
    const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{ .{ .key = "entity/x", .target = "entity/b" }, .{ .key = "entity/y", .target = "entity/e" } }, .state = .{ .distinct = &.{.{ .from = "entity/x", .to = "entity/other" }} } };
    try store.adoptNamespace(initial);
    while (!try store.advanceQueryPins(template, id, lookup, 128)) {}
    const state_path = try std.mem.concat(a, u8, &.{ try store.pinProjectionRoot(template, id), "state" });
    var legacy = try parse(Store.PinProjection, a, memory.rows.get(state_path).?);
    legacy.format = 2;
    legacy.sequence = null;
    try store.backend.put(store.backend.ptr, a, state_path, try encode(a, legacy));
    const journal = try std.mem.concat(a, u8, &.{ try store.pinEditRoot(template), "head" });
    try store.backend.put(store.backend.ptr, a, journal, try encode(a, .{ .sequence = @as(u64, 0), .graph = legacy.graph }));
    try std.testing.expect(!try store.queryPinsIndexed(template, id));
    var turns: usize = 0;
    while (!try store.advanceQueryPins(template, id, lookup, 32)) : (turns += 1) try std.testing.expect(turns < 128);
    try std.testing.expect(try store.queryPinsIndexed(template, id));
    try std.testing.expect((try store.pinEditHead(template, legacy.graph)).indexed);
    var old_journal = try store.pinEditHead(template, legacy.graph);
    old_journal.routing_indexed = false;
    old_journal.routing_origin = null;
    try store.backend.put(store.backend.ptr, a, journal, try encode(a, old_journal));
    try std.testing.expect(!try store.advanceQueryPins(template, id, lookup, 32));
    const upgraded_journal = try store.pinEditHead(template, legacy.graph);
    try std.testing.expect(upgraded_journal.routing_indexed);
    try std.testing.expect(upgraded_journal.routing_origin != null);
    try std.testing.expectEqual(old_journal.sequence, upgraded_journal.barrier);
    while (!try store.advanceQueryPins(template, id, lookup, 32)) {}

    var redirected = initial;
    redirected.revision += 1;
    const burst = try a.alloc(resolver.KeyAlias, 42);
    burst[0] = .{ .key = "entity/x", .target = "entity/d" };
    burst[1] = initial.merges[1];
    for (burst[2..], 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/guard-noise-{d}", .{i}), .target = "entity/guard-noise-root" };
    redirected.merges = burst;
    try store.apply(try difference(a, initial, redirected));
    {
        const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = lookup }, true)).?;
        defer prepared.release();
        var roots: std.StringHashMapUnmanaged(void) = .empty;
        try roots.put(a, "entity/e", {});
        var starts: std.StringHashMapUnmanaged(void) = .empty;
        // A configured fallback that remains masked is not an affected root.
        try std.testing.expect(try store.projectedQueryPins(prepared, roots, lookup, &starts));
    }
    var next = redirected;
    next.revision += 1;
    next.merges = &.{.{ .key = "entity/y", .target = "entity/e" }};
    try store.apply(try difference(a, redirected, next));
    turns = 0;
    while (true) : (turns += 1) {
        const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = lookup }, true)).?;
        defer prepared.release();
        for ([_][]const u8{ "entity/e", "entity/b", "entity/c", "entity/x" }, 0..) |seed, index| {
            const result = try store.queryExecutionPrepared(prepared, &.{seed}, &.{}, true);
            try std.testing.expectEqual(@as(usize, if (index == 0 or index == 3) 1 else 0), result.namespace.state.distinct.len);
        }
        const before = memory.writes;
        const done = try store.advanceQueryPins(template, id, lookup, 32);
        try std.testing.expect(memory.writes - before <= 33);
        try std.testing.expect(turns < 256);
        if (done) break;
    }
    try std.testing.expect(try store.queryPinsIndexed(template, id));
    var previous = next;
    for (0..48) |step| {
        var changed = previous;
        changed.revision += 1;
        const edges = try a.alloc(resolver.KeyAlias, 2);
        edges[0] = next.merges[0];
        edges[1] = .{ .key = "entity/churn", .target = try std.fmt.allocPrint(a, "entity/transient-{d}", .{step}) };
        changed.merges = edges;
        try store.apply(try difference(a, previous, changed));
        var maintenance: usize = 0;
        while (!try store.advanceQueryPins(template, id, lookup, 128)) : (maintenance += 1) try std.testing.expect(maintenance < 4);
        const paths = try store.backend.scan(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ try store.pinProjectionRoot(template, id), "path/" }), null, 128);
        try std.testing.expect(paths.rows.len <= 8);
        const current_path = try std.fmt.allocPrint(a, "{s}path/{s}", .{ try store.pinProjectionRoot(template, id), try hash(a, "entity/churn") });
        try std.testing.expect(memory.rows.get(current_path) == null);
        previous = changed;
    }
    // Updating a masked edge's configured parent must move its latent guard,
    // even though effective reverse routing does not include that edge.
    var parent_changed = previous;
    parent_changed.revision += 1;
    parent_changed.merges = &.{ .{ .key = "entity/x", .target = "entity/b" }, .{ .key = "entity/y", .target = "entity/f" } };
    try store.apply(try difference(a, previous, parent_changed));
    turns = 0;
    while (!try store.advanceQueryPins(template, id, lookup, 128)) : (turns += 1) try std.testing.expect(turns < 128);
    const guard_key = try std.fmt.allocPrint(a, "{s}guard/{s}", .{ try store.pinProjectionRoot(template, id), try hash(a, "entity/x") });
    const guard = try parse(Store.PinGuard, a, memory.rows.get(guard_key).?);
    try std.testing.expect(guard.latent);
    try std.testing.expectEqualStrings("entity/f", guard.root);
    var unmasked = parent_changed;
    unmasked.revision += 1;
    unmasked.merges = &.{parent_changed.merges[1]};
    try store.apply(try difference(a, parent_changed, unmasked));
    const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = lookup }, true)).?;
    defer prepared.release();
    var roots: std.StringHashMapUnmanaged(void) = .empty;
    try roots.put(a, "entity/f", {});
    var starts: std.StringHashMapUnmanaged(void) = .empty;
    try std.testing.expect(!try store.projectedQueryPins(prepared, roots, lookup, &starts));
    try std.testing.expectEqual(@as(usize, 1), (try store.queryExecutionPrepared(prepared, &.{"entity/f"}, &.{}, true)).namespace.state.distinct.len);
    var rows = memory.rows.iterator();
    while (rows.next()) |row| try validatePortableRow(a, row.key_ptr.*, row.value_ptr.*);
}

test "resolver curation dependency repair progresses under ongoing unrelated churn" {
    for ([_]u8{ 0, 1, 2 }) |mode| {
        const churn = mode != 0;
        const count: usize = 128;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const aliases = try a.alloc(resolver.KeyAlias, count * 2);
        const pairs = try a.alloc(c.Pair, count);
        for (pairs, 0..) |*pair, i| {
            aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
            aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = "entity/unpinned-root" };
            pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
        }
        const template = "entity/{{_key}}";
        const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs } };
        try store.adoptNamespace(initial);
        const Txn = struct {
            memory: *OrderedPinMemory,
            pub fn get(self: *@This(), k: []const u8) ![]const u8 {
                return self.memory.rows.get(k) orelse error.NotFound;
            }
            pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
                return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
            }
            pub fn delete(self: *@This(), k: []const u8) !void {
                return OrderedPinMemory.remove(self.memory, self.memory.a, k);
            }
        };
        var txn: Txn = .{ .memory = &memory };
        const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
        const id = try definition.identity(a);
        try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
        var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
        var turns: usize = 0;
        while (true) : (turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const writes = memory.writes;
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - writes <= 129);
            try std.testing.expect(turns < (pairs.len * 32 + 111) / 112 + 32);
            if (done) break;
        }
        var max_scans: usize = 0;
        for (0..3) |_| {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var current = store;
            current.a = request.allocator();
            const prepared = (try current.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
            defer prepared.release();
            const before = memory.scans;
            const result = try current.queryExecutionPrepared(prepared, &.{"entity/unpinned-root"}, &.{}, true);
            max_scans = @max(max_scans, memory.scans - before);
            try std.testing.expect(memory.scans - before <= 12);
            try std.testing.expect(request.queryCapacity() < 128 * 1024);
            try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
        }
        var namespace = initial;
        namespace.revision += 1;
        namespace.merges = &.{.{ .key = "entity/pinned-root", .target = "entity/moved-root" }};
        try store.apply(try difference(a, initial, namespace));
        var ready = false;
        var owner_writes: usize = 0;
        var owner_turns: usize = 0;
        for (0..128) |step| {
            if (churn) {
                var next = namespace;
                next.revision += 1;
                const edges = try a.alloc(resolver.KeyAlias, if (mode == 2) 17 else 2);
                edges[0] = namespace.merges[0];
                edges[1] = .{ .key = "entity/background", .target = if (step % 2 == 0) "entity/bg-a" else "entity/bg-b" };
                if (mode == 2) for (edges[1..], 0..) |*edge, i| {
                    edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/background-{d}", .{i}), .target = if (step % 2 == 0) "entity/bg-a" else "entity/bg-b" };
                };
                next.merges = edges;
                try store.apply(try difference(a, namespace, next));
                namespace = next;
            }
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const before = memory.writes;
            ready = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - before <= 129);
            owner_writes += memory.writes - before;
            owner_turns += 1;
            if (ready) break;
        }
        const root = try store.pinProjectionRoot(template, id);
        const state = try parse(Store.PinProjection, a, memory.rows.get(try std.mem.concat(a, u8, &.{ root, "state" })).?);
        const members = try store.backend.scan(store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}member/{s}/", .{ root, try hash(a, "entity/moved-root") }), null, 512);
        std.debug.print("DEPENDENCY CHURN mode={d} pins={d} ready={} turns={d} owner_writes={d} phase={s} moved_members={d}\n", .{ mode, count, ready, owner_turns, owner_writes, @tagName(state.phase), members.rows.len });
        // Sixteen fresh routes per turn exceed the cost of fully repairing
        // that background component, but cannot starve the older component.
        if (mode != 2) try std.testing.expect(ready);
        try std.testing.expectEqual(count, members.rows.len);
        var recovery: usize = 0;
        while (!try store.advanceQueryPins(template, id, reader.lookup(), 128)) : (recovery += 1) try std.testing.expect(recovery < 256);
        std.debug.print("DEPENDENCY CHURN recovery_turns={d}\n", .{recovery + 1});
    }
}

test "resolver curation dependency compact proof bounds distinct configured invalidations" {
    for ([_]usize{ 256, 4096 }) |count| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const aliases = try a.alloc(resolver.KeyAlias, count * 2);
        const pairs = try a.alloc(c.Pair, count);
        for (pairs, 0..) |*pair, i| {
            aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
            aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = "entity/unpinned-root" };
            pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
        }
        const template = "entity/{{_key}}";
        const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs } };
        try store.adoptNamespace(initial);
        const Txn = struct {
            memory: *OrderedPinMemory,
            pub fn get(self: *@This(), k: []const u8) ![]const u8 {
                return self.memory.rows.get(k) orelse error.NotFound;
            }
            pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
                return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
            }
            pub fn delete(self: *@This(), k: []const u8) !void {
                return OrderedPinMemory.remove(self.memory, self.memory.a, k);
            }
        };
        var txn: Txn = .{ .memory = &memory };
        const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
        const id = try definition.identity(a);
        try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
        var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
        var turns: usize = 0;
        while (true) : (turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const writes = memory.writes;
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - writes <= 129);
            try std.testing.expect(turns < (pairs.len * 32 + 111) / 112 + 32);
            if (done) break;
        }
        var max_scans: usize = 0;
        for (0..3) |_| {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var current = store;
            current.a = request.allocator();
            const prepared = (try current.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
            defer prepared.release();
            const before = memory.scans;
            const result = try current.queryExecutionPrepared(prepared, &.{"entity/unpinned-root"}, &.{}, true);
            max_scans = @max(max_scans, memory.scans - before);
            try std.testing.expect(memory.scans - before <= 12);
            try std.testing.expect(request.queryCapacity() < 128 * 1024);
            try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
        }
        var namespace = initial;
        for (0..count / 128) |step| {
            var next = namespace;
            next.revision += 1;
            const edges = try a.alloc(resolver.KeyAlias, (step + 1) * 128);
            @memcpy(edges[0..namespace.merges.len], namespace.merges);
            for (0..128) |i| edges[step * 128 + i] = .{ .key = aliases[step * 128 + i].key, .target = "entity/curated-root" };
            next.merges = edges;
            try store.apply(try difference(a, namespace, next));
            namespace = next;
        }
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var current = store;
        current.a = request.allocator();
        const prepared = (try current.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer prepared.release();
        const before = memory.scans;
        const result = try current.queryExecutionPrepared(prepared, &.{"entity/unpinned-root"}, &.{}, true);
        try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
        try std.testing.expect(memory.scans - before <= 8);
        try std.testing.expect(request.queryCapacity() < 64 * 1024);
        std.debug.print("DEPENDENCY DISTINCT configured_keys={d} scans={d} request_arena={d} pairs={d}\n", .{ count, memory.scans - before, request.queryCapacity(), result.namespace.state.distinct.len });
        var bytes: [64 * 1024]u8 = undefined;
        var bounded: std.heap.FixedBufferAllocator = .init(&bytes);
        var constrained = store;
        constrained.a = bounded.allocator();
        const prepared_small = (try constrained.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer prepared_small.release();
        const small_result = try constrained.queryExecutionPrepared(prepared_small, &.{"entity/unpinned-root"}, &.{}, true);
        try std.testing.expectEqual(@as(usize, 0), small_result.namespace.state.distinct.len);
        std.debug.print("DEPENDENCY DISTINCT constrained64k=success used={d}\n", .{bounded.end_index});
        if (count == 256) {
            // Root-local helper allocations must reset even if every summary
            // is empty and the point-lookup branch never executes.
            var absent_roots: std.StringHashMapUnmanaged(void) = .empty;
            for (0..8192) |i| try absent_roots.put(a, try std.fmt.allocPrint(a, "entity/absent-root-{d}", .{i}), {});
            const projection = try parse(Store.PinProjection, a, memory.rows.get(try std.mem.concat(a, u8, &.{ try store.pinProjectionRoot(template, id), "state" })).?);
            try std.testing.expect(try current.pinRootsCurrent(prepared, projection, absent_roots, reader.lookup()));
            try std.testing.expect(request.queryCapacity() < 64 * 1024);
        }
        var owner_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer owner_arena.deinit();
        var probe_owner = store;
        probe_owner.a = owner_arena.allocator();
        const writes_before = memory.writes;
        const event_reads_before = memory.pin_event_reads;
        _ = try probe_owner.advanceQueryPins(template, id, reader.lookup(), 128);
        try std.testing.expect(owner_arena.queryCapacity() < 512 * 1024);
        try std.testing.expect(memory.pin_event_reads - event_reads_before <= 2);
        try std.testing.expect(memory.writes - writes_before <= 129);
        std.debug.print("DEPENDENCY OWNER count={d} arena={d} writes={d}\n", .{ count, owner_arena.queryCapacity(), memory.writes - writes_before });
        var owner_bytes: [512 * 1024]u8 = undefined;
        var owner_frame: std.heap.FixedBufferAllocator = .init(&owner_bytes);
        var small_owner = store;
        small_owner.a = owner_frame.allocator();
        _ = try small_owner.advanceQueryPins(template, id, reader.lookup(), 128);
        try std.testing.expect(owner_frame.end_index < 512 * 1024);
        std.debug.print("DEPENDENCY OWNER bounded512k=success used={d}\n", .{owner_frame.end_index});
    }
}

test "resolver curation dependency summaries preserve collisions and reclaim references" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const backend: Backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan };
    const index: dependency_filter.Index = .{ .a = a, .backend = backend, .root = "filter/" };
    const root = try hash(a, "root");
    const digest: [32]u8 = @splat(0);
    for (0..24) |_| {
        const before = memory.writes;
        try index.update(root, &digest, true);
        try std.testing.expect(memory.writes - before <= 4);
    }
    var collision = digest;
    collision[31] = 1;
    var reader = (try index.reader(a, root)).?;
    // Fingerprint collisions are positive hints; the caller checks full SHA.
    try std.testing.expect(try reader.contains(a, &collision));
    for (0..23) |_| try index.update(root, &digest, false);
    reader = (try index.reader(a, root)).?;
    try std.testing.expect(try reader.contains(a, &digest));
    try index.update(root, &digest, false);
    var empty_bytes: [4096]u8 = undefined;
    var empty_frame: std.heap.FixedBufferAllocator = .init(&empty_bytes);
    try std.testing.expect(try index.reader(empty_frame.allocator(), root) == null);
    try std.testing.expectEqual(@as(usize, 0), memory.rows.count());
}

test "resolver curation dependency paged summaries split merge and reject absent fingerprints" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const backend: Backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan };
    const index: dependency_filter.Index = .{ .a = a, .backend = backend, .root = "filter/" };
    const root = try hash(a, "paged-root");
    const digests = try a.alloc([32]u8, 1000);
    var buffer: [96]u8 = undefined;
    for (digests, 0..) |*digest, i| {
        @import("antfly_hash").Sha256.hash(try std.fmt.bufPrint(&buffer, "entity/member-{d}", .{i}), digest, .{});
        const before = memory.writes;
        try index.update(root, digest, true);
        try std.testing.expect(memory.writes - before <= 4);
    }
    var reader = (try index.reader(a, root)).?;
    var page_bytes: [256 * 1024]u8 = undefined;
    var frame: std.heap.FixedBufferAllocator = .init(&page_bytes);
    for (digests) |*digest| {
        frame.reset();
        try std.testing.expect(try reader.contains(frame.allocator(), digest));
    }
    const absent = try a.alloc([32]u8, 1000);
    for (absent, 0..) |*digest, i| @import("antfly_hash").Sha256.hash(try std.fmt.bufPrint(&buffer, "entity/absent-{d}", .{i}), digest, .{});
    std.mem.sortUnstable([32]u8, absent, {}, struct {
        fn less(_: void, l: [32]u8, r: [32]u8) bool {
            return std.mem.lessThan(u8, &l, &r);
        }
    }.less);
    const scans = memory.scans;
    for (absent) |*digest| {
        frame.reset();
        try std.testing.expect(!try reader.contains(frame.allocator(), digest));
    }
    try std.testing.expect(memory.scans - scans < 128);
    var collision = digests[0];
    collision[31] ^= 1;
    try index.update(root, &collision, true);
    try index.update(root, &digests[0], false);
    reader = (try index.reader(a, root)).?;
    frame.reset();
    try std.testing.expect(try reader.contains(frame.allocator(), &collision));
    for (digests[1..]) |*digest| {
        const before = memory.writes;
        try index.update(root, digest, false);
        try std.testing.expect(memory.writes - before <= 4);
    }
    reader = (try index.reader(a, root)).?;
    frame.reset();
    try std.testing.expect(try reader.contains(frame.allocator(), &collision));
    try index.update(root, &collision, false);
    try std.testing.expectEqual(@as(usize, 0), memory.rows.count());
}

test "resolver curation dependency projection enrollment ignores unrelated native namespaces" {
    for ([_]usize{ 0, 1000, 6000 }) |population| {
        const count: usize = 8;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const aliases = try a.alloc(resolver.KeyAlias, count * 2);
        const pairs = try a.alloc(c.Pair, count);
        for (pairs, 0..) |*pair, i| {
            aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
            aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = "entity/unpinned-root" };
            pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
        }
        const template = "entity/{{_key}}";
        const native_edges = try a.alloc(resolver.KeyAlias, population);
        for (native_edges, 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/unrelated-native-{d}", .{i}), .target = "entity/unrelated-native-root" };
        const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = native_edges, .state = .{ .distinct = pairs } };
        try store.adoptNamespace(initial);
        const Txn = struct {
            memory: *OrderedPinMemory,
            pub fn get(self: *@This(), k: []const u8) ![]const u8 {
                return self.memory.rows.get(k) orelse error.NotFound;
            }
            pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
                return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
            }
            pub fn delete(self: *@This(), k: []const u8) !void {
                return OrderedPinMemory.remove(self.memory, self.memory.a, k);
            }
        };
        var txn: Txn = .{ .memory = &memory };
        const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
        const id = try definition.identity(a);
        try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
        var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
        const writes_before = memory.writes;
        var turns: usize = 0;
        while (true) : (turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const writes = memory.writes;
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - writes <= 129);
            try std.testing.expect(turns < 8192);
            if (done) break;
        }
        const projection_root = try store.pinProjectionRoot(template, id);
        var paths: usize = 0;
        var summary_bytes: usize = 0;
        var profile_bytes: usize = 0;
        var rows = memory.rows.iterator();
        while (rows.next()) |row| {
            if (!std.mem.startsWith(u8, row.key_ptr.*, projection_root)) continue;
            profile_bytes += row.key_ptr.*.len + row.value_ptr.*.len;
            if (std.mem.indexOf(u8, row.key_ptr.*, "/path/") != null) paths += 1;
            if (std.mem.indexOf(u8, row.key_ptr.*, "/dep-summary/") != null) summary_bytes += row.value_ptr.*.len;
        }
        try std.testing.expect(paths <= aliases.len + 4);
        try std.testing.expect(memory.writes - writes_before < 512);
        try std.testing.expect(turns < 16);
        std.debug.print("SCOPED PROJECTION configured_aliases={d} unrelated_native={d} turns={d} writes={d} paths={d} summary_bytes={d} profile_bytes={d}\n", .{ aliases.len, population, turns + 1, memory.writes - writes_before, paths, summary_bytes, profile_bytes });
        if (population != 0) {
            var next = initial;
            next.revision += 1;
            const next_edges = try a.dupe(resolver.KeyAlias, native_edges);
            for (next_edges[0..64]) |*edge| edge.target = "entity/other-unrelated-native-root";
            next.merges = next_edges;
            try store.apply(try difference(a, initial, next));
            const before = memory.writes;
            var repair_turns: usize = 0;
            while (!try store.advanceQueryPins(template, id, reader.lookup(), 128)) : (repair_turns += 1) try std.testing.expect(repair_turns < 8);
            try std.testing.expect(memory.writes - before <= 8);
            std.debug.print("SCOPED EDIT unrelated_native={d} changed_routes=64 writes={d} turns={d}\n", .{ population, memory.writes - before, repair_turns + 1 });
        }
    }
}

test "resolver curation dependency small root summaries remain compact" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const aliases = try a.alloc(resolver.KeyAlias, 6000);
    const pairs = try a.alloc(c.Pair, aliases.len);
    const pad = try a.alloc(u8, 128);
    @memset(pad, 'x');
    for (aliases, pairs, 0..) |*alias, *pair, i| {
        alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/pin-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/root-{d}-{s}", .{ i, pad }) };
        pair.* = .{ .from = alias.key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
    }
    const template = "entity/{{_key}}";
    const namespace: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs } };
    try store.adoptNamespace(namespace);
    const Txn = struct {
        memory: *OrderedPinMemory,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.memory.rows.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            return OrderedPinMemory.put(self.memory, self.memory.a, key, value);
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            return OrderedPinMemory.remove(self.memory, self.memory.a, key);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
    const id = try definition.identity(a);
    try static_profiles.synchronize(a, &txn, "pin-profiles/", &.{}, &.{definition});
    var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "pin-profiles/", .profile = id, .indexed = true };
    var bootstrap_turns: usize = 0;
    while (!try store.advanceQueryPins(template, id, reader.lookup(), 128)) : (bootstrap_turns += 1) try std.testing.expect(bootstrap_turns < 2048);
    const projection_root = try store.pinProjectionRoot(template, id);
    var counters: usize = 0;
    var summary_bytes: usize = 0;
    var paths: usize = 0;
    var profile_bytes: usize = 0;
    var rows = memory.rows.iterator();
    while (rows.next()) |row| {
        if (!std.mem.startsWith(u8, row.key_ptr.*, projection_root)) continue;
        profile_bytes += row.key_ptr.*.len + row.value_ptr.*.len;
        if (std.mem.indexOf(u8, row.key_ptr.*, "/dep-summary/") != null) {
            counters += 1;
            summary_bytes += row.value_ptr.*.len;
        }
        if (std.mem.indexOf(u8, row.key_ptr.*, "/path/") != null) paths += 1;
    }
    try std.testing.expect(counters <= 6000);
    try std.testing.expect(summary_bytes < 1024 * 1024);
    std.debug.print("SPARSE SUMMARIES roots=6000 paths={d} summary_rows={d} summary_bytes={d} profile_bytes={d}\n", .{ paths, counters, summary_bytes, profile_bytes });
}

test "resolver curation dependency wide repeated edits probe only distinct hashes" {
    const count: usize = 128;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const aliases = try a.alloc(resolver.KeyAlias, count * 2);
    const pairs = try a.alloc(c.Pair, count);
    for (pairs, 0..) |*pair, i| {
        aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
        aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/unpinned-root-{d}", .{i % 64}) };
        pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
    }
    const template = "entity/{{_key}}";
    const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs } };
    try store.adoptNamespace(initial);
    const Txn = struct {
        memory: *OrderedPinMemory,
        pub fn get(self: *@This(), k: []const u8) ![]const u8 {
            return self.memory.rows.get(k) orelse error.NotFound;
        }
        pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
            return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
        }
        pub fn delete(self: *@This(), k: []const u8) !void {
            return OrderedPinMemory.remove(self.memory, self.memory.a, k);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
    const id = try definition.identity(a);
    try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
    var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
    try std.testing.expect(!try store.queryPinsIndexed(template, id));
    var turns: usize = 0;
    while (true) : (turns += 1) {
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var owner = store;
        owner.a = request.allocator();
        const writes = memory.writes;
        const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
        try std.testing.expect(memory.writes - writes <= 129);
        try std.testing.expect(turns < (pairs.len * 32 + 111) / 112 + 32);
        if (done) break;
    }
    var namespace = initial;
    for (0..64) |step| {
        var next = namespace;
        next.revision += 1;
        const edges = try a.alloc(resolver.KeyAlias, count);
        for (edges, 0..) |*edge, i| edge.* = .{ .key = aliases[i].key, .target = if (step % 2 == 0) "entity/curated-a" else "entity/curated-b" };
        next.merges = edges;
        try store.apply(try difference(a, namespace, next));
        namespace = next;
    }
    const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
    defer prepared.release();
    var roots: [64][]const u8 = undefined;
    for (&roots, 0..) |*root, i| root.* = try std.fmt.allocPrint(a, "entity/unpinned-root-{d}", .{i});
    test_pin_summary_probes.store(0, .monotonic);
    const reads_before = memory.reads;
    const scans_before = memory.scans;
    const result = try store.queryExecutionPrepared(prepared, &roots, &.{}, true);
    const probes = test_pin_summary_probes.load(.monotonic);
    try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
    const invalidations = try store.backend.scan(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ try store.pinEditRoot(template), "invalidated/" }), null, 1000);
    try std.testing.expectEqual(@as(usize, 130), invalidations.rows.len);
    try std.testing.expect(probes <= roots.len * invalidations.rows.len);
    std.debug.print("UNIQUE DIGEST PROOF events=64 queried_roots=64 unique_invalidations={d} digest_probes={d} reads={d} scans={d} pairs={d}\n", .{ invalidations.rows.len, probes, memory.reads - reads_before, memory.scans - scans_before, result.namespace.state.distinct.len });
}

test "resolver curation dependency digest union fits the full retained ring in bounded scratch" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var seeds: [130][32]u8 = undefined;
    var buffer: [64]u8 = undefined;
    for (&seeds, 0..) |*seed, i| @import("antfly_hash").Sha256.hash(try std.fmt.bufPrint(&buffer, "entity/hash-{d}", .{i}), seed, .{});
    @memcpy(seeds[1][0..8], seeds[0][0..8]); // Same hash-table bucket, different full SHA.
    const bytes = try a.alloc(u8, 256 * 640 * 32);
    const digests = std.mem.bytesAsSlice([32]u8, bytes);
    for (digests, 0..) |*digest, i| digest.* = seeds[i % seeds.len];
    const table_bytes = try a.alloc(u8, 1024 * 1024);
    var frame: std.heap.FixedBufferAllocator = .init(table_bytes);
    const size = try Store.uniquePinDigests(bytes, &frame);
    try std.testing.expectEqual(@as(usize, 130 * 32), size);
    const unique = std.mem.bytesAsSlice([32]u8, bytes[0..size]);
    for (unique[1..], 0..) |digest, i| try std.testing.expect(std.mem.lessThan(u8, &unique[i], &digest));
}

test "resolver curation dependency large root summaries retain selective fingerprints" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const index: dependency_filter.Index = .{ .a = a, .root = "filter/", .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const root = try hash(a, "large-root");
    var buffer: [96]u8 = undefined;
    var digest: [32]u8 = undefined;
    for (0..100000) |i| {
        @import("antfly_hash").Sha256.hash(try std.fmt.bufPrint(&buffer, "entity/root-member-{d}", .{i}), &digest, .{});
        const before = memory.writes;
        try index.update(root, &digest, true);
        try std.testing.expect(memory.writes - before <= 4);
    }
    var row_bytes: usize = 0;
    var rows = memory.rows.iterator();
    while (rows.next()) |row| row_bytes += row.value_ptr.*.len;
    try std.testing.expect(row_bytes < 2 * 1024 * 1024);
    const queries = try a.alloc([32]u8, 10000);
    for (queries, 0..) |*query, i| @import("antfly_hash").Sha256.hash(try std.fmt.bufPrint(&buffer, "entity/absent-member-{d}", .{i}), query, .{});
    std.mem.sortUnstable([32]u8, queries, {}, struct {
        fn less(_: void, l: [32]u8, r: [32]u8) bool {
            return std.mem.lessThan(u8, &l, &r);
        }
    }.less);
    var reader = (try index.reader(a, root)).?;
    const page_bytes = try a.alloc(u8, 512 * 1024);
    var frame: std.heap.FixedBufferAllocator = .init(page_bytes);
    var positives: usize = 0;
    const before_scans = memory.scans;
    for (queries) |*query| {
        frame.reset();
        if (try reader.contains(frame.allocator(), query)) positives += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), positives);
    try std.testing.expect(memory.scans - before_scans < 128);
    std.debug.print("LARGE SUMMARY members=100000 absent=10000 positives={d} scans={d} summary_bytes={d}\n", .{ positives, memory.scans - before_scans, row_bytes });
}

test "resolver curation dependency fences newly reachable native tails and prunes orphan paths" {
    for ([_]u2{ 0, 1, 2 }) |mode| {
        const late_tail = mode != 0;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const aliases = [_]resolver.KeyAlias{ .{ .key = "entity/a", .target = "entity/z" }, .{ .key = "entity/b", .target = if (mode == 2) "entity/r" else "entity/q" } };
        const Reader = struct {
            nodes: query_context.NodeIndex,
            aliases: []const resolver.KeyAlias,
            fn node(ptr: *anyopaque, _: A, id: []const u8) !?query_context.QueryNode {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return self.nodes.by_key.get(id);
            }
            fn seek(ptr: *anyopaque, _: A, digest: []const u8, root: ?[]const u8) !?query_context.QueryNode {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return self.nodes.seek(digest, root);
            }
            fn expand(ptr: *anyopaque, alloc: A, id: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                for (self.aliases) |edge| if (std.mem.eql(u8, edge.key, id)) return alloc.dupe(resolver.KeyAlias, &.{edge});
                return &.{};
            }
        };
        var reader: Reader = .{ .nodes = try query_context.NodeIndex.init(a, &aliases), .aliases = &aliases };
        const lookup: AliasLookup = .{ .ptr = &reader, .node = Reader.node, .seek_node = Reader.seek, .expand = Reader.expand };
        const id = try hash(a, "scoped-incoming-tail");
        const template = "entity/{{_key}}";
        var previous: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = if (mode == 2) &.{.{ .key = "entity/q", .target = "entity/r" }} else if (late_tail) &.{} else &.{.{ .key = "entity/y", .target = "entity/q" }}, .state = .{ .distinct = &.{.{ .from = "entity/a", .to = "entity/other" }} } };
        try store.adoptNamespace(previous);
        while (!try store.advanceQueryPins(template, id, lookup, 128)) {}
        const projection = try store.pinProjectionRoot(template, id);
        const tail_path = try std.fmt.allocPrint(a, "{s}path/{s}", .{ projection, try hash(a, "entity/y") });
        try std.testing.expect(memory.rows.get(tail_path) == null);
        var next = previous;
        next.revision += 1;
        next.merges = if (mode == 2) &.{ .{ .key = "entity/q", .target = "entity/r" }, .{ .key = "entity/z", .target = "entity/y" } } else if (late_tail) &.{.{ .key = "entity/z", .target = "entity/y" }} else &.{ .{ .key = "entity/y", .target = "entity/q" }, .{ .key = "entity/z", .target = "entity/y" } };
        try store.apply(try difference(a, previous, next));
        previous = next;
        if (late_tail) {
            next.revision += 1;
            next.merges = if (mode == 2) &.{ .{ .key = "entity/q", .target = "entity/r" }, .{ .key = "entity/y", .target = "entity/q" }, .{ .key = "entity/z", .target = "entity/y" } } else &.{ .{ .key = "entity/y", .target = "entity/q" }, .{ .key = "entity/z", .target = "entity/y" } };
            try store.apply(try difference(a, previous, next));
            previous = next;
        }
        for (0..2) |stage| {
            var turns: usize = 0;
            while (true) : (turns += 1) {
                const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = lookup }, true)).?;
                defer prepared.release();
                const result = try store.queryExecutionPrepared(prepared, &.{if (mode == 2) "entity/r" else "entity/q"}, &.{}, true);
                try std.testing.expectEqual(@as(usize, if (stage == 0) 1 else 0), result.namespace.state.distinct.len);
                const before = memory.writes;
                const done = try store.advanceQueryPins(template, id, lookup, 32);
                try std.testing.expect(memory.writes - before <= 33);
                try std.testing.expect(turns < 128);
                if (done) break;
            }
            if (stage == 0) {
                try std.testing.expect(memory.rows.get(tail_path) != null);
                next.revision += 1;
                // The native y -> q declaration survives, but its profile path
                // must disappear when no configured node can reach it.
                next.merges = if (mode == 2) &.{ .{ .key = "entity/q", .target = "entity/r" }, .{ .key = "entity/y", .target = "entity/q" }, .{ .key = "entity/z", .target = "entity/elsewhere" } } else &.{ .{ .key = "entity/y", .target = "entity/q" }, .{ .key = "entity/z", .target = "entity/elsewhere" } };
                try store.apply(try difference(a, previous, next));
                previous = next;
            }
        }
        try std.testing.expect(memory.rows.get(tail_path) == null);
        var rows = memory.rows.iterator();
        while (rows.next()) |row| try validatePortableRow(a, row.key_ptr.*, row.value_ptr.*);
    }
}

test "resolver curation dependency tail journal deduplicates converging chains and expires by ownership" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const template = "entity/{{_key}}";
    const edges = try a.alloc(resolver.KeyAlias, 1024);
    for (edges, 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/tail-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/tail-{d}", .{i + 1}) };
    try store.adoptNamespace(.{ .key_template = template, .revision = 1, .merges = edges });
    var loader: Store.InlineQueryProfile = .{ .aliases = &.{} };
    const prepared = (try store.prepareQuery(template, .{ .id = "", .ptr = &loader, .load = Store.InlineQueryProfile.load }, true)).?;
    defer prepared.release();
    const journal = try store.pinEditRoot(template);
    try store.backend.put(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ journal, "head" }), try encode(a, Store.PinEditHead{ .graph = prepared.graph_id, .indexed = true, .compact = true, .tails_indexed = true, .routing_indexed = true, .routing_origin = prepared.graph_id }));
    // Repeat every target in many events: one descriptor per target, never one
    // copy per retained slot. Old-slot expiration cannot delete newer owners.
    const targets = try a.alloc([]const u8, 128);
    for (targets, 0..) |*key, i| key.* = edges[i].key;
    for (0..300) |_| try store.appendPinEdit(template, prepared.graph_id, prepared.graph_id, &.{}, targets, targets);
    const prefix = try std.mem.concat(a, u8, &.{ journal, "tails/" });
    const page = try store.backend.scan(store.backend.ptr, a, prefix, null, 256);
    try std.testing.expectEqual(@as(usize, 128), page.rows.len);
    var roots: std.StringHashMapUnmanaged(void) = .empty;
    try roots.put(a, "entity/unrelated", {});
    const scratch = try query_context.RoutingScratch.init();
    defer scratch.deinit();
    var events: std.heap.FixedBufferAllocator = .init(scratch.bytes[6 * 1024 * 1024 .. 7 * 1024 * 1024]);
    var points: std.heap.FixedBufferAllocator = .init(scratch.bytes[7 * 1024 * 1024 ..]);
    var memo: std.heap.FixedBufferAllocator = .init(scratch.bytes[5 * 1024 * 1024 .. 6 * 1024 * 1024]);
    const Empty = struct {
        fn expand(_: *anyopaque, _: A, _: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
            return &.{};
        }
    };
    const lookup: AliasLookup = .{ .ptr = &loader, .expand = Empty.expand };
    const reads = memory.reads;
    try std.testing.expect(try store.incomingPinRootsCurrent(prepared, 299, 300, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
    // All 128 entry points share the same thousand-hop suffix. Read it once.
    try std.testing.expect(memory.reads - reads < 1400);
    try roots.put(a, "entity/tail-1024", {});
    try std.testing.expect(!try store.incomingPinRootsCurrent(prepared, 299, 300, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
    for (0..256) |_| try store.appendPinEdit(template, prepared.graph_id, prepared.graph_id, &.{}, &.{}, &.{});
    try std.testing.expectEqual(@as(usize, 0), (try store.backend.scan(store.backend.ptr, a, prefix, null, 256)).rows.len);
    try std.testing.expectEqual(@as(usize, 0), (try store.backend.scan(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ journal, "tail-key/" }), null, 256)).rows.len);
    inline for (.{ "routed/", "routed-key/" }) |suffix| try std.testing.expectEqual(@as(usize, 0), (try store.backend.scan(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ journal, suffix }), null, 256)).rows.len);
    var rows = memory.rows.iterator();
    while (rows.next()) |row| try validatePortableRow(a, row.key_ptr.*, row.value_ptr.*);
}

test "resolver curation dependency enrollment excludes unrelated native pin counts" {
    for ([_]usize{ 0, 1000, 6000 }) |population| {
        const count: usize = 8;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const aliases = try a.alloc(resolver.KeyAlias, count * 2);
        const pairs = try a.alloc(c.Pair, count);
        for (pairs, 0..) |*pair, i| {
            aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
            aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = "entity/unpinned-root" };
            pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
        }
        const template = "entity/{{_key}}";
        const native_edges = try a.alloc(resolver.KeyAlias, 0);
        for (native_edges, 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/unrelated-native-{d}", .{i}), .target = "entity/unrelated-native-root" };
        const extra_pairs = try a.alloc(c.Pair, population);
        for (extra_pairs, 0..) |*pair, i| pair.* = .{ .from = try std.fmt.allocPrint(a, "entity/foreign-pin-a-{d}", .{i}), .to = try std.fmt.allocPrint(a, "entity/foreign-pin-b-{d}", .{i}) };
        const all_pairs = try std.mem.concat(a, c.Pair, &.{ pairs, extra_pairs });
        const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = native_edges, .state = .{ .distinct = all_pairs } };
        try store.adoptNamespace(initial);
        const Txn = struct {
            memory: *OrderedPinMemory,
            pub fn get(self: *@This(), k: []const u8) ![]const u8 {
                return self.memory.rows.get(k) orelse error.NotFound;
            }
            pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
                return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
            }
            pub fn delete(self: *@This(), k: []const u8) !void {
                return OrderedPinMemory.remove(self.memory, self.memory.a, k);
            }
        };
        var txn: Txn = .{ .memory = &memory };
        const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
        const id = try definition.identity(a);
        try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
        var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
        const writes_before = memory.writes;
        const reads_before = memory.reads;
        const scans_before = memory.scans;
        const visited_before = memory.visited;
        var turns: usize = 0;
        while (true) : (turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const writes = memory.writes;
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - writes <= 129);
            try std.testing.expect(turns < 8192);
            if (done) break;
        }
        const projection_root = try store.pinProjectionRoot(template, id);
        var paths: usize = 0;
        var summary_bytes: usize = 0;
        var profile_bytes: usize = 0;
        var rows = memory.rows.iterator();
        while (rows.next()) |row| {
            if (!std.mem.startsWith(u8, row.key_ptr.*, projection_root)) continue;
            profile_bytes += row.key_ptr.*.len + row.value_ptr.*.len;
            if (std.mem.indexOf(u8, row.key_ptr.*, "/path/") != null) paths += 1;
            if (std.mem.indexOf(u8, row.key_ptr.*, "/dep-summary/") != null) summary_bytes += row.value_ptr.*.len;
        }
        try std.testing.expect(paths <= aliases.len + 4);
        try std.testing.expect(turns < 8);
        try std.testing.expect(memory.reads - reads_before < 512);
        try std.testing.expect(memory.writes - writes_before < 256);

        std.debug.print("SPARSE PIN ENROLL configured_aliases={d} unrelated_native_pairs={d} turns={d} writes={d} paths={d} summary_bytes={d} profile_bytes={d} reads={d} scans={d} visited_rows={d}\n", .{ aliases.len, population, turns + 1, memory.writes - writes_before, paths, summary_bytes, profile_bytes, memory.reads - reads_before, memory.scans - scans_before, memory.visited - visited_before });

        if (population == 6000) {
            // Simulate a paused format-5 writer whose old namespace-wide pass
            // cursor is an unrelated native posting between configured nodes.
            const nodes = try query_context.NodeIndex.init(a, aliases);
            const lower = nodes.nodes[nodes.nodes.len / 2 - 1].digest;
            const upper = nodes.nodes[nodes.nodes.len / 2].digest;
            const pin_prefix = try store.keyPath(try std.fmt.allocPrint(a, "pin-count/{s}/", .{try hash(a, template)}));
            const native = (try store.backend.scan(store.backend.ptr, a, pin_prefix, try std.mem.concat(a, u8, &.{ pin_prefix, &lower }), 1)).rows[0];
            const cursor = native.key[pin_prefix.len..];
            try std.testing.expect(std.mem.lessThan(u8, cursor, &upper));
            try std.testing.expect(!nodes.by_key.contains((try parse(Store.PinCount, a, native.value)).key));
            const member_prefix = try std.fmt.allocPrint(a, "{s}member/{s}/", .{ projection_root, try hash(a, "entity/pinned-root") });
            const before_members = try store.backend.scan(store.backend.ptr, a, member_prefix, null, 128);
            var removed: usize = 0;
            for (before_members.rows) |row| {
                const member = try parse(Store.PinMember, a, row.value);
                const digest = try hash(a, member.key);
                if (!std.mem.lessThan(u8, cursor, digest)) continue;
                try store.backend.remove(store.backend.ptr, a, row.key);
                try store.backend.remove(store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}boundary/{s}", .{ projection_root, digest }));
                removed += 1;
            }
            try std.testing.expect(removed != 0);
            const state_key = try std.mem.concat(a, u8, &.{ projection_root, "state" });
            var paused = try parse(Store.PinProjection, a, memory.rows.get(state_key).?);
            paused.phase = .build;
            paused.after = cursor;
            try store.backend.put(store.backend.ptr, a, state_key, try encode(a, paused));
            try std.testing.expect(!try store.queryPinsIndexed(template, id));
            var resumed: usize = 0;
            while (true) : (resumed += 1) {
                const writes = memory.writes;
                const done = try store.advanceQueryPins(template, id, reader.lookup(), 32);
                try std.testing.expect(memory.writes - writes <= 33);
                try std.testing.expect(resumed < 8);
                if (done) break;
            }
            try std.testing.expectEqual(count, (try store.backend.scan(store.backend.ptr, a, member_prefix, null, 128)).rows.len);
            const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
            defer prepared.release();
            try std.testing.expectEqual(count, (try store.queryExecutionPrepared(prepared, &.{"entity/pinned-root"}, &.{}, true)).namespace.state.distinct.len);
            std.debug.print("SPARSE PIN CURSOR RESUME native_posting_cursor=true restored={d} turns={d}\n", .{ removed, resumed + 1 });
        }
    }
}

test "resolver curation dependency tail canonical cache reuses work and fences routing" {
    for ([_]usize{ 1024, 8192 }) |count| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const template = "entity/{{_key}}";
        const edges = try a.alloc(resolver.KeyAlias, count);
        for (edges, 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/tail-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/tail-{d}", .{i + 1}) };
        const second = try a.alloc(resolver.KeyAlias, count);
        for (second, 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/other-tail-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/other-tail-{d}", .{i + 1}) };
        const all = try std.mem.concat(a, resolver.KeyAlias, &.{ edges, second });
        try store.adoptNamespace(.{ .key_template = template, .revision = 1, .merges = all });
        var loader: Store.InlineQueryProfile = .{ .aliases = &.{} };
        const profile = try (static_profiles.Definition{ .key_template = template }).identity(a);
        const prepared = (try store.prepareQuery(template, .{ .id = profile, .ptr = &loader, .load = Store.InlineQueryProfile.load }, true)).?;
        defer prepared.release();
        for ([_][]const u8{ edges[0].key, second[0].key }) |key| {
            const projection = try store.pinProjectionRoot(template, profile);
            try store.backend.put(store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}path/{s}", .{ projection, try hash(a, key) }), try encode(a, Store.PinPath{ .key = key, .root = "entity/unrelated-terminal" }));
        }
        const journal = try store.pinEditRoot(template);
        try store.backend.put(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ journal, "head" }), try encode(a, Store.PinEditHead{ .graph = prepared.graph_id, .indexed = true, .compact = true, .tails_indexed = true, .routing_indexed = true, .routing_origin = prepared.graph_id }));
        try store.appendPinEdit(template, prepared.graph_id, prepared.graph_id, &.{}, &.{edges[0].key}, &.{edges[0].key});
        var roots: std.StringHashMapUnmanaged(void) = .empty;
        try roots.put(a, "entity/unrelated", {});
        const scratch = try query_context.RoutingScratch.init();
        defer scratch.deinit();
        var events: std.heap.FixedBufferAllocator = .init(scratch.bytes[6 * 1024 * 1024 .. 7 * 1024 * 1024]);
        var points: std.heap.FixedBufferAllocator = .init(scratch.bytes[7 * 1024 * 1024 ..]);
        var memo: std.heap.FixedBufferAllocator = .init(scratch.bytes[5 * 1024 * 1024 .. 6 * 1024 * 1024]);
        const Empty = struct {
            fn expand(_: *anyopaque, _: A, _: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
                return &.{};
            }
        };
        const lookup: AliasLookup = .{ .ptr = &loader, .expand = Empty.expand };
        for (0..2) |repeat| {
            const reads = memory.reads;
            const scans = memory.scans;
            try std.testing.expect(try store.incomingPinRootsCurrent(prepared, 0, 1, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
            if (repeat == 1) try std.testing.expect(memory.reads - reads < 16);
            std.debug.print("CACHED NATIVE TAIL hops={d} repeat={d} reads={d} scans={d}\n", .{ count, repeat, memory.reads - reads, memory.scans - scans });
        }
        try store.appendPinEdit(template, prepared.graph_id, prepared.graph_id, &.{}, &.{second[0].key}, &.{second[0].key});
        const before = memory.reads;
        const indexed = try store.incomingPinRootsCurrent(prepared, 0, 2, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]);
        try std.testing.expect(indexed);
        try std.testing.expect(memory.reads - before <= count + 32);
        std.debug.print("CACHED DISJOINT TAILS each_hops={d} indexed={any} reads={d}\n", .{ count, indexed, memory.reads - before });
        const warm_reads = memory.reads;
        try std.testing.expect(try store.incomingPinRootsCurrent(prepared, 0, 2, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
        try std.testing.expect(memory.reads - warm_reads < 16);
        // A cold proof can run out of its global work quantum, but completed
        // immutable entries survive: the next read finishes instead of starting over.
        var cold = prepared;
        cold.profile_id = try (static_profiles.Definition{ .key_template = template, .suppress_json = "{\"keys\":[\"entity/cold-profile\"]}" }).identity(a);
        for ([_][]const u8{ edges[0].key, second[0].key }) |key| {
            const projection = try store.pinProjectionRoot(template, cold.profile_id);
            try store.backend.put(store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}path/{s}", .{ projection, try hash(a, key) }), try encode(a, Store.PinPath{ .key = key, .root = "entity/unrelated-terminal" }));
        }
        const cold_reads = memory.reads;
        const cold_indexed = try store.incomingPinRootsCurrent(cold, 0, 2, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]);
        try std.testing.expectEqual(count < 8192, cold_indexed);
        try std.testing.expect(memory.reads - cold_reads <= 16416);
        const retry_reads = memory.reads;
        try std.testing.expect(try store.incomingPinRootsCurrent(cold, 0, 2, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
        try std.testing.expect(memory.reads - retry_reads <= count + 32);
        const steady_reads = memory.reads;
        try std.testing.expect(try store.incomingPinRootsCurrent(cold, 0, 2, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
        try std.testing.expect(memory.reads - steady_reads < 16);
        std.debug.print("TAIL COLD RECOVERY each_hops={d} cold_indexed={any} cold_reads={d} retry_reads={d} steady_reads={d}\n", .{ count, cold_indexed, retry_reads - cold_reads, steady_reads - retry_reads, memory.reads - steady_reads });
        // Mutating a native edge changes the routing lineage. Old cached
        // terminals cannot authorize the new graph, even for the same profile.
        const original: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = all };
        var noise = original;
        noise.revision += 1;
        noise.merges = try std.mem.concat(a, resolver.KeyAlias, &.{ all, &.{.{ .key = "entity/unrelated-cache-edit", .target = "entity/unrelated-cache-target" }} });
        try store.apply(try difference(a, original, noise));
        const noisy = (try store.prepareQuery(template, .{ .id = profile, .ptr = &loader, .load = Store.InlineQueryProfile.load }, true)).?;
        defer noisy.release();
        const steps_before = test_tail_forward_steps.load(.monotonic);
        const noise_reads = memory.reads;
        try std.testing.expect(try store.incomingPinRootsCurrent(noisy, 0, 3, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
        try std.testing.expectEqual(steps_before, test_tail_forward_steps.load(.monotonic));
        try std.testing.expect(memory.reads - noise_reads < 32);
        var changed = noise;
        changed.revision += 1;
        const updated = try a.dupe(resolver.KeyAlias, noise.merges);
        updated[count - 1].target = "entity/rebound";
        changed.merges = updated;
        try store.apply(try difference(a, noise, changed));
        const refreshed = (try store.prepareQuery(template, .{ .id = profile, .ptr = &loader, .load = Store.InlineQueryProfile.load }, true)).?;
        defer refreshed.release();
        try std.testing.expect(!std.mem.eql(u8, &refreshed.routing_id, &prepared.routing_id));
        try roots.put(a, "entity/rebound", {});
        const invalidation_reads = memory.reads;
        try std.testing.expect(!try store.incomingPinRootsCurrent(refreshed, 0, 4, roots, lookup, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
        try std.testing.expect(memory.reads - invalidation_reads >= count);
    }
}

test "resolver curation dependency ingestion resets item scratch for valid maximum keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const template = "entity/{{_key}}";
    const canonical = try a.alloc(u8, 16384);
    @memcpy(canonical[0..7], "entity/");
    @memset(canonical[7..], 1);
    try c.validKey(canonical);
    const id = try hash(a, "valid-key-item-scratch");
    const keys = try a.alloc([]const u8, 128);
    for (keys, 0..) |*key, i| {
        key.* = try std.fmt.allocPrint(a, "entity/small-{d}", .{i});
        try store.backend.put(store.backend.ptr, a, try std.fmt.allocPrint(a, "static-root-profile/query-node/{s}/{s}", .{ id, try hash(a, key.*) }), try encode(a, query_context.QueryNode{ .key = key.*, .canonical = canonical }));
    }
    var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
    const graph: [32]u8 = @splat(1);
    const journal = try store.pinEditRoot(template);
    try store.backend.put(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ journal, "head" }), try encode(a, Store.PinEditHead{ .graph = graph, .indexed = true, .compact = true, .tails_indexed = true, .routing_indexed = true, .routing_origin = graph }));
    try store.appendPinEdit(template, graph, graph, keys, &.{}, &.{});
    var state: Store.PinProjection = .{ .graph = graph, .format = Store.pin_projection_format, .phase = .repair, .sequence = 0, .repair = .{ .sequence = 1, .target = 0 } };
    const scratch = try query_context.RoutingScratch.init();
    defer scratch.deinit();
    var frame: std.heap.FixedBufferAllocator = .init(scratch.bytes);
    var budget: u32 = 128;
    try store.ingestPinEdits(template, "projection/", &state, try store.pinEditHead(template, graph), reader.lookup(), &budget, &frame);
    try std.testing.expectEqual(@as(u16, 127), state.repair.?.index);
    budget = 128;
    try store.ingestPinEdits(template, "projection/", &state, try store.pinEditHead(template, graph), reader.lookup(), &budget, &frame);
    try std.testing.expectEqual(@as(u64, 2), state.repair.?.sequence);
    try std.testing.expectEqual(@as(usize, 128), (try store.backend.scan(store.backend.ptr, a, "projection/next-boundary/", null, 256)).rows.len);
    // The same item scratch covers routing and target lookups, including a
    // larger supported owner quantum spanning an entire mixed event.
    try store.appendPinEdit(template, graph, graph, &.{}, keys, keys);
    budget = 4096;
    try store.ingestPinEdits(template, "projection/", &state, try store.pinEditHead(template, graph), reader.lookup(), &budget, &frame);
    try std.testing.expectEqual(@as(u64, 3), state.repair.?.sequence);
    try std.testing.expectEqual(@as(usize, 128), (try store.backend.scan(store.backend.ptr, a, "projection/next-node/", null, 256)).rows.len);
    std.debug.print("BOUNDED ITEM SCRATCH valid_canonical_bytes={d} event_keys={d} completed_sequence={d} remaining_budget={d}\n", .{ canonical.len, keys.len, state.repair.?.sequence, budget });
}

test "resolver curation dependency tail canonical cache isolates configured profiles and survives admission denial" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const template = "entity/{{_key}}";
    try store.adoptNamespace(.{ .key_template = template, .revision = 1, .merges = &.{.{ .key = "entity/profile-tail", .target = "entity/profile-stop" }} });
    const aliases_a = [_]resolver.KeyAlias{.{ .key = "entity/profile-stop", .target = "entity/profile-a" }};
    const aliases_b = [_]resolver.KeyAlias{.{ .key = "entity/profile-stop", .target = "entity/profile-b" }};
    var input_a: Store.InlineQueryProfile = .{ .aliases = &aliases_a };
    var input_b: Store.InlineQueryProfile = .{ .aliases = &aliases_b };
    const prepared_a = (try store.prepareQuery(template, .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = &aliases_a }).identity(a), .ptr = &input_a, .load = Store.InlineQueryProfile.load }, true)).?;
    defer prepared_a.release();
    const prepared_b = (try store.prepareQuery(template, .{ .id = try (static_profiles.Definition{ .key_template = template, .aliases = &aliases_b }).identity(a), .ptr = &input_b, .load = Store.InlineQueryProfile.load }, true)).?;
    defer prepared_b.release();
    const Edges = struct {
        aliases: []const resolver.KeyAlias,
        fn expand(ptr: *anyopaque, alloc: A, key: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (self.aliases) |edge| if (std.mem.eql(u8, edge.key, key)) return alloc.dupe(resolver.KeyAlias, &.{edge});
            return &.{};
        }
    };
    var edges_a: Edges = .{ .aliases = &aliases_a };
    var edges_b: Edges = .{ .aliases = &aliases_b };
    const lookup_a: AliasLookup = .{ .ptr = &edges_a, .expand = Edges.expand };
    const lookup_b: AliasLookup = .{ .ptr = &edges_b, .expand = Edges.expand };
    for ([_]Store.PreparedQuery{ prepared_a, prepared_b }) |prepared| {
        const projection = try store.pinProjectionRoot(template, prepared.profile_id);
        const key = "entity/profile-tail";
        try store.backend.put(store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}path/{s}", .{ projection, try hash(a, key) }), try encode(a, Store.PinPath{ .key = key, .root = "entity/profile-stop" }));
    }
    const journal = try store.pinEditRoot(template);
    try store.backend.put(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ journal, "head" }), try encode(a, Store.PinEditHead{ .graph = prepared_a.graph_id, .indexed = true, .compact = true, .tails_indexed = true, .routing_indexed = true, .routing_origin = prepared_a.graph_id }));
    try store.appendPinEdit(template, prepared_a.graph_id, prepared_a.graph_id, &.{}, &.{"entity/profile-tail"}, &.{"entity/profile-tail"});
    var roots: std.StringHashMapUnmanaged(void) = .empty;
    try roots.put(a, "entity/profile-a", {});
    const scratch = try query_context.RoutingScratch.init();
    defer scratch.deinit();
    var events: std.heap.FixedBufferAllocator = .init(scratch.bytes[6 * 1024 * 1024 .. 7 * 1024 * 1024]);
    var points: std.heap.FixedBufferAllocator = .init(scratch.bytes[7 * 1024 * 1024 ..]);
    var memo: std.heap.FixedBufferAllocator = .init(scratch.bytes[5 * 1024 * 1024 .. 6 * 1024 * 1024]);
    // A leased oversized routing result denies new cache reservations. Exact
    // bounded proofs must remain available, with no cached proof contamination.
    const Pressure = struct {
        fn load(_: *anyopaque, alloc: A) !query_context.Graph {
            const value = try alloc.alloc(u8, 17 * 1024 * 1024);
            @memset(value, 'p');
            const list = try alloc.alloc(query_context.Route, 1);
            list[0] = .{ .root = value, .component = "pressure" };
            return .{ .routes = list };
        }
    };
    var marker: u8 = 0;
    var pressure_key: [32]u8 = undefined;
    @import("antfly_hash").Sha256.hash("tail-cache-admission-pressure", &pressure_key, .{});
    {
        const pressure = (try query_context.tryRouting(pressure_key, .{ .ptr = &marker, .load = Pressure.load })).?;
        defer pressure.release();
        for (0..2) |_| {
            const reads = memory.reads;
            try std.testing.expect(!try store.incomingPinRootsCurrent(prepared_a, 0, 1, roots, lookup_a, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
            try std.testing.expect(try store.incomingPinRootsCurrent(prepared_b, 0, 1, roots, lookup_b, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
            try std.testing.expect(memory.reads - reads != 0);
        }
    }
    try std.testing.expect(!try store.incomingPinRootsCurrent(prepared_a, 0, 1, roots, lookup_a, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
    try std.testing.expect(try store.incomingPinRootsCurrent(prepared_b, 0, 1, roots, lookup_b, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
    // Poison the caller's scratch: retained terminals belong to immutable
    // cache leases, never to a previous page or a lookup's reset buffer.
    @memset(points.buffer, 0xaa);
    const reads = memory.reads;
    try std.testing.expect(!try store.incomingPinRootsCurrent(prepared_a, 0, 1, roots, lookup_a, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
    try std.testing.expect(try store.incomingPinRootsCurrent(prepared_b, 0, 1, roots, lookup_b, &events, &points, &memo, scratch.bytes[0 .. 5 * 1024 * 1024]));
    try std.testing.expect(memory.reads - reads < 16);
    std.debug.print("TAIL CACHE profile_isolation=qualified admission_denial=exact warm_reads={d}\n", .{memory.reads - reads});
}

test "resolver curation dependency membership join skips large configured gaps and resumes removed pins" {
    const Reader = struct {
        aliases: []const resolver.KeyAlias,
        nodes: query_context.NodeIndex,
        fn node(ptr: *anyopaque, _: A, key: []const u8) !?query_context.QueryNode {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.nodes.by_key.get(key);
        }
        fn seek(ptr: *anyopaque, _: A, digest: []const u8, canonical: ?[]const u8) !?query_context.QueryNode {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var result = self.nodes.seek(digest, canonical);
            // Legacy callbacks can omit the optional digest. The join must
            // hash the returned key instead of trusting a precomputed field.
            if (result) |*value| value.digest = @splat(0);
            return result;
        }
        fn expand(ptr: *anyopaque, a: A, key: []const u8, _: bool, _: EdgeMask) ![]const resolver.KeyAlias {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (self.aliases) |edge| if (std.mem.eql(u8, key, edge.key)) return a.dupe(resolver.KeyAlias, &.{edge});
            return &.{};
        }
        fn lookup(self: *@This()) AliasLookup {
            return .{ .ptr = self, .node = node, .seek_node = seek, .expand = expand };
        }
    };
    for ([_]usize{ 1000, 10000 }) |count| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const template = "entity/{{_key}}";
        const aliases = try a.alloc(resolver.KeyAlias, count);
        for (aliases, 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/sparse-join-{d}", .{i}), .target = "entity/sparse-join-root" };
        const pair_list = [_]c.Pair{ .{ .from = aliases[0].key, .to = "entity/other-first" }, .{ .from = aliases[count - 1].key, .to = "entity/other-last" } };
        try store.adoptNamespace(.{ .key_template = template, .revision = 1, .state = .{ .distinct = &pair_list } });
        var reader: Reader = .{ .aliases = aliases, .nodes = try query_context.NodeIndex.init(a, aliases) };
        const id = try (static_profiles.Definition{ .key_template = template, .aliases = aliases }).identity(a);
        var turns: usize = 0;
        while (true) : (turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const writes = memory.writes;
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - writes <= 129);
            try std.testing.expect(turns < count * 2);
            if (done) break;
        }
        const projection = try store.pinProjectionRoot(template, id);
        for (pair_list) |pair| {
            try store.backend.remove(store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}boundary/{s}", .{ projection, try hash(a, pair.from) }));
            try store.backend.remove(store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}member/{s}/{s}", .{ projection, try hash(a, "entity/sparse-join-root"), try hash(a, pair.from) }));
        }
        const state_path = try std.mem.concat(a, u8, &.{ projection, "state" });
        var state = try parse(Store.PinProjection, a, memory.rows.get(state_path).?);
        state.phase = .build;
        state.after = null;
        try store.backend.put(store.backend.ptr, a, state_path, try encode(a, state));
        const reads = memory.reads;
        const scans = memory.scans;
        try std.testing.expect(try store.advanceQueryPins(template, id, reader.lookup(), 128));
        try std.testing.expect(memory.reads - reads < 32);
        try std.testing.expect(memory.scans - scans < 32);
        const members = try store.backend.scan(store.backend.ptr, a, try std.fmt.allocPrint(a, "{s}member/{s}/", .{ projection, try hash(a, "entity/sparse-join-root") }), null, 128);
        try std.testing.expectEqual(@as(usize, 2), members.rows.len);
        std.debug.print("SPARSE CONFIGURED JOIN aliases={d} pins=2 reads={d} scans={d}\n", .{ count, memory.reads - reads, memory.scans - scans });
    }
    {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const template = "entity/{{_key}}";
        const aliases = [_]resolver.KeyAlias{ .{ .key = "entity/resume-a", .target = "entity/resume-b" }, .{ .key = "entity/resume-c", .target = "entity/resume-d" } };
        var reader: Reader = .{ .aliases = &aliases, .nodes = try query_context.NodeIndex.init(a, &aliases) };
        const id = try (static_profiles.Definition{ .key_template = template, .aliases = &aliases }).identity(a);
        const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = &.{.{ .key = "entity/unrelated-native-join", .target = "entity/unrelated-native-root" }}, .state = .{ .distinct = &.{ .{ .from = aliases[0].key, .to = "entity/old-other" }, .{ .from = aliases[1].key, .to = "entity/kept-other" } } } };
        try store.adoptNamespace(initial);
        while (!try store.advanceQueryPins(template, id, reader.lookup(), 128)) {}
        const projection = try store.pinProjectionRoot(template, id);
        const state_path = try std.mem.concat(a, u8, &.{ projection, "state" });
        var state = try parse(Store.PinProjection, a, memory.rows.get(state_path).?);
        state.phase = .build;
        state.after = null;
        state.walk = .{ .key = aliases[0].key, .current = aliases[0].key, .root = aliases[0].target };
        try store.tracePinNode(projection, aliases[0].key);
        try store.backend.put(store.backend.ptr, a, state_path, try encode(a, state));
        var changed = initial;
        changed.revision += 1;
        changed.state.distinct = initial.state.distinct[1..];
        try store.apply(try difference(a, initial, changed));
        var turns: usize = 0;
        while (true) : (turns += 1) {
            const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
            defer prepared.release();
            try std.testing.expectEqual(@as(usize, 0), (try store.queryExecutionPrepared(prepared, &.{aliases[0].target}, &.{}, true)).namespace.state.distinct.len);
            try std.testing.expectEqual(@as(usize, 1), (try store.queryExecutionPrepared(prepared, &.{aliases[1].target}, &.{}, true)).namespace.state.distinct.len);
            const writes = memory.writes;
            const done = try store.advanceQueryPins(template, id, reader.lookup(), 32);
            try std.testing.expect(memory.writes - writes <= 33);
            try std.testing.expect(turns < 64);
            if (done) break;
        }
        try std.testing.expect(memory.rows.get(try std.fmt.allocPrint(a, "{s}boundary/{s}", .{ projection, try hash(a, aliases[0].key) })) == null);
        std.debug.print("SPARSE JOIN paused_unpin=exact turns={d}\n", .{turns + 1});
    }
}

test "resolver curation dependency adaptive intersection bounds distinct edits for tiny summaries" {
    for ([_]usize{ 16, 128 }) |event_count| {
        const count: usize = 128;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var memory: OrderedPinMemory = .{ .a = a };
        const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
        const aliases = try a.alloc(resolver.KeyAlias, count * 2);
        const pairs = try a.alloc(c.Pair, count);
        for (pairs, 0..) |*pair, i| {
            aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
            aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/unpinned-root-{d}", .{i % 64}) };
            pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
        }
        const template = "entity/{{_key}}";
        const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .state = .{ .distinct = pairs } };
        try store.adoptNamespace(initial);
        const Txn = struct {
            memory: *OrderedPinMemory,
            pub fn get(self: *@This(), k: []const u8) ![]const u8 {
                return self.memory.rows.get(k) orelse error.NotFound;
            }
            pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
                return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
            }
            pub fn delete(self: *@This(), k: []const u8) !void {
                return OrderedPinMemory.remove(self.memory, self.memory.a, k);
            }
        };
        var txn: Txn = .{ .memory = &memory };
        const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
        const id = try definition.identity(a);
        try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
        var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
        try std.testing.expect(!try store.queryPinsIndexed(template, id));
        var turns: usize = 0;
        while (true) : (turns += 1) {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            var owner = store;
            owner.a = request.allocator();
            const writes = memory.writes;
            const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
            try std.testing.expect(memory.writes - writes <= 129);
            try std.testing.expect(turns < (pairs.len * 32 + 111) / 112 + 32);
            if (done) break;
        }

        for (0..event_count) |step| {
            var request = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer request.deinit();
            const alloc = request.allocator();
            var writer = store;
            writer.a = alloc;
            const old = (try writer.head(template)).?;
            var next = old;
            next.revision += 1;
            const edits = try alloc.alloc(Edit, count);
            for (edits, 0..) |*edit, i| {
                const edge: resolver.KeyAlias = .{ .key = try std.fmt.allocPrint(alloc, "entity/disjoint-edit-{d}-{d}", .{ step, i }), .target = "entity/disjoint-target" };
                edit.* = .{ .kind = .redirect, .id = edge.key, .value = try parse(std.json.Value, alloc, try encode(alloc, edge)) };
            }
            try writer.apply(.{ .expected_revision = old.revision, .next = next, .edits = edits });
        }
        const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer prepared.release();
        var roots: [64][]const u8 = undefined;
        for (&roots, 0..) |*root, i| root.* = try std.fmt.allocPrint(a, "entity/unpinned-root-{d}", .{i});
        test_pin_summary_probes.store(0, .monotonic);
        const reads_before = memory.reads;
        const scans_before = memory.scans;
        const result = try store.queryExecutionPrepared(prepared, &roots, &.{}, true);
        const probes = test_pin_summary_probes.load(.monotonic);
        try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
        const invalidations = try store.backend.scan(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ try store.pinEditRoot(template), "invalidated/" }), null, 20000);
        try std.testing.expectEqual(event_count * count + 1, invalidations.rows.len);
        try std.testing.expect(probes <= roots.len * 3);
        std.debug.print("ADAPTIVE TINY SUMMARY events={d} queried_roots=64 dependencies_per_root=3 unique_invalidations={d} digest_probes={d} reads={d} scans={d} pairs={d}\n", .{ event_count, invalidations.rows.len, probes, memory.reads - reads_before, memory.scans - scans_before, result.namespace.state.distinct.len });
    }
}

test "resolver curation dependency unrelated routing churn preserves bounded profile proofs" {
    const count: usize = 128;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const aliases = try a.alloc(resolver.KeyAlias, count * 2);
    const pairs = try a.alloc(c.Pair, count);
    for (pairs, 0..) |*pair, i| {
        aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
        aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/unpinned-root-{d}", .{i % 64}) };
        pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
    }
    const template = "entity/{{_key}}";

    const chain_length: usize = 8192;
    const native = try a.alloc(resolver.KeyAlias, 2 * chain_length);
    for (native[0..chain_length], 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/review-tail-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/review-tail-{d}", .{i + 1}) };
    for (native[chain_length..], 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "entity/review-other-tail-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/review-other-tail-{d}", .{i + 1}) };
    const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = native, .state = .{ .distinct = pairs } };
    try store.adoptNamespace(initial);
    const Txn = struct {
        memory: *OrderedPinMemory,
        pub fn get(self: *@This(), k: []const u8) ![]const u8 {
            return self.memory.rows.get(k) orelse error.NotFound;
        }
        pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
            return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
        }
        pub fn delete(self: *@This(), k: []const u8) !void {
            return OrderedPinMemory.remove(self.memory, self.memory.a, k);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
    const id = try definition.identity(a);
    try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
    var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
    try std.testing.expect(!try store.queryPinsIndexed(template, id));
    var turns: usize = 0;
    while (true) : (turns += 1) {
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var owner = store;
        owner.a = request.allocator();
        const writes = memory.writes;
        const done = try owner.advanceQueryPins(template, id, reader.lookup(), 128);
        try std.testing.expect(memory.writes - writes <= 129);
        try std.testing.expect(turns < (pairs.len * 32 + 111) / 112 + 32);
        if (done) break;
    }

    const Mutation = struct {
        fn apply(current: Store, key: []const u8, target: []const u8) !void {
            const h = (try current.head("entity/{{_key}}")).?;
            var next = h;
            next.revision += 1;
            const edge: resolver.KeyAlias = .{ .key = key, .target = target };
            try current.apply(.{ .expected_revision = h.revision, .next = next, .edits = &.{.{ .kind = .redirect, .id = key, .value = try parse(std.json.Value, current.a, try encode(current.a, edge)) }} });
        }
    };
    try Mutation.apply(store, "entity/unrelated-bridge-a", native[0].key);
    try Mutation.apply(store, "entity/unrelated-bridge-b", native[chain_length].key);
    var warm_reads: usize = 0;
    for (0..3) |repeat| {
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var current = store;
        current.a = request.allocator();
        const prepared = (try current.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer prepared.release();
        const before = memory.reads;
        const result = try current.queryExecutionPrepared(prepared, &.{"entity/unpinned-root-0"}, &.{}, true);
        try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
        warm_reads = memory.reads - before;
        std.debug.print("SCOPED ROUTING CHURN baseline_repeat={d} native_reads={d} pairs={d}\n", .{ repeat, warm_reads, result.namespace.state.distinct.len });
    }
    try std.testing.expect(warm_reads < 100);
    for (0..3) |step| {
        var request = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer request.deinit();
        var current = store;
        current.a = request.allocator();
        try Mutation.apply(current, try std.fmt.allocPrint(current.a, "entity/review-noise-{d}", .{step}), "entity/review-noise-root");
        const prepared = (try current.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer prepared.release();
        const before = memory.reads;
        const result = try current.queryExecutionPrepared(prepared, &.{"entity/unpinned-root-0"}, &.{}, true);
        try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
        try std.testing.expect(memory.reads - before < 100);
        std.debug.print("SCOPED ROUTING CHURN unrelated_edit={d} native_reads={d} warm_baseline_reads={d} pairs={d}\n", .{ step, memory.reads - before, warm_reads, result.namespace.state.distinct.len });
    }
}

test "resolver curation dependency valid long-key fallback bounds per-hop scratch" {
    const count: usize = 128;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const aliases = try a.alloc(resolver.KeyAlias, count * 2);
    const pairs = try a.alloc(c.Pair, count);
    for (pairs, 0..) |*pair, i| {
        aliases[i] = .{ .key = try std.fmt.allocPrint(a, "entity/pinned-{d}", .{i}), .target = "entity/pinned-root" };
        aliases[count + i] = .{ .key = try std.fmt.allocPrint(a, "entity/unpinned-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/unpinned-root-{d}", .{i % 64}) };
        pair.* = .{ .from = aliases[i].key, .to = try std.fmt.allocPrint(a, "entity/other-{d}", .{i}) };
    }
    const template = "entity/{{_key}}";

    const chain_count: usize = 1800;
    const native_keys = try a.alloc([]const u8, chain_count + 1);
    for (native_keys, 0..) |*key, i| {
        const raw = try a.alloc(u8, 1000);
        const label = try std.fmt.bufPrint(raw, "entity/review-long-{d:0>4}/", .{i});
        @memset(raw[label.len..], 'x');
        key.* = raw;
        try c.validKey(raw);
    }
    const native = try a.alloc(resolver.KeyAlias, chain_count);
    for (native, 0..) |*edge, i| edge.* = .{ .key = native_keys[i], .target = native_keys[i + 1] };
    aliases[0].target = native_keys[0];
    const initial: policy.Namespace = .{ .key_template = template, .revision = 1, .merges = native, .state = .{ .distinct = pairs } };
    try store.adoptNamespace(initial);
    const Txn = struct {
        memory: *OrderedPinMemory,
        pub fn get(self: *@This(), k: []const u8) ![]const u8 {
            return self.memory.rows.get(k) orelse error.NotFound;
        }
        pub fn put(self: *@This(), k: []const u8, v: []const u8) !void {
            return OrderedPinMemory.put(self.memory, self.memory.a, k, v);
        }
        pub fn delete(self: *@This(), k: []const u8) !void {
            return OrderedPinMemory.remove(self.memory, self.memory.a, k);
        }
    };
    var txn: Txn = .{ .memory = &memory };
    const definition: static_profiles.Definition = .{ .key_template = template, .aliases = aliases };
    const id = try definition.identity(a);
    try static_profiles.synchronize(a, &txn, "static-root-profile/", &.{}, &.{definition});
    var reader: static_profiles.QueryReader = .{ .backend = store.backend, .root = "static-root-profile/", .profile = id, .indexed = true };
    try std.testing.expect(!try store.queryPinsIndexed(template, id));

    const head = (try store.head(template)).?;
    try std.testing.expect(head.bytes < c.max_state_bytes);
    const prepared = (try store.prepareQuery(template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
    defer prepared.release();
    const result = try store.queryExecutionPrepared(prepared, &.{"entity/unpinned-root-0"}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 0), result.namespace.state.distinct.len);
    // Anonymous profiles deliberately bypass membership-cache admission and
    // exercise the streaming fallback with the same immutable lookup.
    var uncached = prepared;
    uncached.profile_id = "";
    const streamed = try store.queryExecutionPrepared(uncached, &.{"entity/unpinned-root-0"}, &.{}, true);
    try std.testing.expectEqual(@as(usize, 0), streamed.namespace.state.distinct.len);
    std.debug.print("BOUNDED LONG FALLBACK namespace_bytes={d} admitted_limit={d} chain_hops={d} key_bytes=1000 unrelated_query=success\n", .{ head.bytes, c.max_state_bytes, chain_count });
}

test "resolver curation dependency adaptive intersection emits all full SHA collision candidates" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const backend: Backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan };
    const index: dependency_filter.Index = .{ .a = a, .backend = backend, .root = "filter/" };
    const root = try hash(a, "collision-root");
    var digests: [4][32]u8 = @splat(@splat(0));
    digests[1][31] = 1;
    digests[2][31] = 2;
    digests[3][0] = 1;
    try index.update(root, &digests[0], true);
    var reader = (try index.reader(a, root)).?;
    var bytes: [4096]u8 = undefined;
    var frame: std.heap.FixedBufferAllocator = .init(&bytes);
    var matches = reader.intersection(&digests);
    for (digests[0..3]) |digest| try std.testing.expectEqual(digest, (try matches.next(&frame)).?.*);
    try std.testing.expect(try matches.next(&frame) == null);
    try std.testing.expectEqual(@as(usize, 1), matches.probes);
    // Force several leaf batches with a tiny I/O frame. An all-negative
    // intersection must recycle it inside next(), not only between matches.
    for (0..512) |i| {
        var digest: [32]u8 = @splat(0);
        std.mem.writeInt(u64, digest[0..8], i + 2, .big);
        try index.update(root, &digest, true);
    }
    const absent = try a.alloc([32]u8, 1024);
    for (absent, 0..) |*digest, i| {
        digest.* = @splat(0);
        std.mem.writeInt(u64, digest[0..8], i + 10000, .big);
    }
    reader = (try index.reader(a, root)).?;
    reader.pages = reader.pages[0..1];
    var paged_bytes: [8192]u8 = undefined;
    var paged_frame: std.heap.FixedBufferAllocator = .init(&paged_bytes);
    matches = reader.intersection(absent);
    try std.testing.expect(try matches.next(&paged_frame) == null);
    try std.testing.expectEqual(@as(usize, 513), matches.probes);
}

test "resolver curation dependency cached path validation reads only missing journal prefixes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory: OrderedPinMemory = .{ .a = a };
    const store: Store = .{ .a = a, .prefix = lite_prefix, .backend = .{ .ptr = &memory, .get = OrderedPinMemory.get, .put = OrderedPinMemory.put, .remove = OrderedPinMemory.remove, .scan = OrderedPinMemory.scan } };
    const template = "entity/{{_key}}";
    const graph: [32]u8 = @splat(11);
    const journal = try store.pinEditRoot(template);
    try store.backend.put(store.backend.ptr, a, try std.mem.concat(a, u8, &.{ journal, "head" }), try encode(a, Store.PinEditHead{ .graph = graph, .indexed = true, .compact = true, .tails_indexed = true, .routing_indexed = true, .routing_origin = graph }));
    for (0..256) |i| try store.appendPinEdit(template, graph, graph, &.{}, &.{if (i == 127 or i == 200) "entity/path-dependency" else "entity/unrelated"}, &.{});
    const journal_head = try store.pinEditHead(template, graph);
    const scratch = try query_context.RoutingScratch.init();
    defer scratch.deinit();
    var frame: std.heap.FixedBufferAllocator = .init(scratch.bytes[6 * 1024 * 1024 ..]);
    var changes: Store.RoutingChanges = .{ .storage = scratch.bytes[0 .. 5 * 1024 * 1024] };
    var digest: [32]u8 = undefined;
    @import("antfly_hash").Sha256.hash("entity/path-dependency", &digest, .{});
    const reads = memory.reads;
    try changes.load(store, template, journal_head, 255, &frame);
    try std.testing.expectEqual(@as(usize, 1), memory.reads - reads);
    try std.testing.expect(!changes.affects(&.{digest}, 255));
    try changes.load(store, template, journal_head, 128, &frame);
    try std.testing.expectEqual(@as(usize, 128), memory.reads - reads);
    try std.testing.expect(changes.affects(&.{digest}, 128));
    try std.testing.expect(!changes.affects(&.{digest}, 201));
    try changes.load(store, template, journal_head, 0, &frame);
    try changes.load(store, template, journal_head, 200, &frame);
    try std.testing.expectEqual(@as(usize, 256), memory.reads - reads);
    try std.testing.expectEqual(@as(usize, 2), changes.entries.len);
}
