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
const resolver = @import("antfly_resolver");
const c = resolver.curation;
const policy = @import("resolver_namespace.zig");
const A = std.mem.Allocator;
pub const static_profiles = @import("resolver_static_profiles.zig");
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
    std.crypto.hash.sha2.Sha256.hash(try encode(a, value), &result, .{});
    return result;
}
fn proposalHeaderValue(a: A, value: c.Proposal) !ProposalHeader {
    var header = value;
    header.evidence = null;
    return .{ .proposal = header, .evidence_hash = try evidenceHash(a, value.evidence), .capacity = c.proposalCapacityBytes(.{ .proposals = &.{value} }) };
}
pub const EdgeMask = struct { ptr: *anyopaque, enabled: *const fn (*anyopaque, A, []const u8) anyerror!bool };
pub const AliasLookup = struct { ptr: *anyopaque, expand: *const fn (*anyopaque, A, []const u8, bool, EdgeMask) anyerror![]const resolver.KeyAlias };
pub const AliasProfile = struct { id: []const u8 = "", key_template: []const u8, aliases: []const resolver.KeyAlias, suppress_json: []const u8 = "" };
/// A request-local key: modern bindings already carry immutable static
/// identity. Only legacy curated overlays need additional content hashing.
pub fn admissionProfileKey(a: A, cfg: anytype) ![]const u8 {
    if (cfg.admission_profile.len > 0 and cfg.merges.len == 0 and cfg.curation_json.len == 0) return cfg.admission_profile;
    var buffer: [1024]u8 = undefined;
    var writer: std.Io.Writer.Hashing(std.crypto.hash.sha2.Sha256) = .init(&buffer);
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

pub const Snapshot = struct { head: Head, namespace: policy.Namespace };
fn parse(comptime T: type, a: A, bytes: []const u8) !T {
    return std.json.parseFromSliceLeaky(T, a, bytes, .{ .allocate = .alloc_always });
}
fn encode(a: A, value: anytype) ![]const u8 {
    return std.json.Stringify.valueAlloc(a, value, .{});
}
fn hash(a: A, value: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
    return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
pub var test_head_enumerations: std.atomic.Value(usize) = .init(0);
pub const Store = struct {
    a: A,
    backend: Backend,
    prefix: []const u8,
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
    fn movePins(self: Store, template: []const u8, start: []const u8, amount: u64, add: bool, detached: std.StringHashMapUnmanaged(void)) !void {
        if (amount == 0) return;
        var id = start;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        while (true) {
            if ((try seen.getOrPut(self.a, id)).found_existing) return error.InvalidResolverAliasCycle;
            const old = try self.pinCount(template, id);
            const next = if (add) try std.math.add(u64, old, amount) else try std.math.sub(u64, old, amount);
            try self.setPinCount(template, id, next);
            if (detached.contains(id)) break;
            const edge = try self.get(resolver.KeyAlias, template, .redirect, id) orelse break;
            try self.pinChild(template, edge.target, id, next != 0);
            id = edge.target;
        }
    }
    /// Query policy is projected at one native read cut. Rendering follows only
    /// forward edges; scoring also needs incident distinct pins and their paths.
    /// Text suppression is always a point lookup, never a namespace export.
    pub fn queryPolicy(self: Store, template: []const u8, seeds: []const []const u8, texts: []const []const u8, static_aliases: []const resolver.KeyAlias, distinct: bool) !?Snapshot {
        var snapshot = (try self.queryForward(template, seeds, static_aliases, 16384)) orelse return null;
        if (distinct) {
            if (try self.backend.get(self.backend.ptr, self.a, try self.pinReadyPath(template))) |raw_ready| {
                if ((try parse(PinReady, self.a, raw_ready)).endpoints != 0) snapshot = try self.pinnedQuery(template, seeds, static_aliases, snapshot);
            } else {
                // Released read-only namespaces retain conservative semantics
                // until an authorized writer adopts the derived directory.
                if ((try self.backend.scan(self.backend.ptr, self.a, try self.records(template, .distinct), null, 1)).rows.len != 0)
                    snapshot = (try self.componentWithAliases(template, seeds, static_aliases)).?;
            }
        }
        var rules: std.ArrayList([]const u8) = .empty;
        var normalized: std.StringHashMapUnmanaged(void) = .empty;
        for (texts) |text| {
            const key = try c.normalize(self.a, text);
            if ((try normalized.getOrPut(self.a, key)).found_existing) continue;
            if (try self.get([]const u8, template, .text, key)) |rule| try rules.append(self.a, rule);
        }
        snapshot.namespace.state.suppress.texts = rules.items;
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
        var graph = initial;
        if (static_aliases.len != 0) {
            var all: std.ArrayList([]const u8) = .empty;
            try all.appendSlice(self.a, seeds);
            for (static_aliases) |alias| try all.append(self.a, alias.key);
            graph = (try self.queryForward(template, all.items, static_aliases, std.math.maxInt(usize))).?;
        }
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
        for (static_aliases) |alias| if (canonical.contains(redirects.canonical(alias.key))) try pending.append(self.a, alias.key);
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
        return self.componentGraph(template, seeds, static_aliases, null);
    }
    pub fn componentIndexed(self: Store, template: []const u8, seeds: []const []const u8, lookup: AliasLookup) !?Snapshot {
        return self.componentGraph(template, seeds, &.{}, lookup);
    }
    fn componentGraph(self: Store, template: []const u8, seeds: []const []const u8, static_aliases: []const resolver.KeyAlias, lookup: ?AliasLookup) !?Snapshot {
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
    pub fn ownsKeyTemplate(self: Store, template: []const u8) !bool {
        if (try self.head(template) != null) return true;
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
            for (try self.proposalIndexes(value)) |index| try self.backend.put(self.backend.ptr, self.a, index, try encode(self.a, value.id));
        }
        h.revision = patch.revision;
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
        std.crypto.hash.sha2.Sha256.hash(try encode(self.a, namespace), &h.fingerprint, .{});
        for (rows) |row| {
            try self.backend.put(self.backend.ptr, self.a, try self.record(h.key_template, row.kind, row.id), row.value);
            try self.indexRecord(h.key_template, row.kind, row.id, row.value, true);
        }
        if (namespace.state.distinct.len == 0) {
            try self.backend.put(self.backend.ptr, self.a, try self.pinReadyPath(h.key_template), try encode(self.a, PinReady{ .endpoints = 0 }));
        } else try self.ensurePinIndex(h.key_template);
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
        try self.preflight(patch);
        const previous = try self.head(patch.next.key_template);
        if ((if (previous) |h| h.revision else 0) != patch.expected_revision or patch.next.revision <= patch.expected_revision) return error.ResolverCurationConflict;
        try self.ensurePinIndex(patch.next.key_template);
        var detached: std.StringHashMapUnmanaged(void) = .empty;
        var endpoints: std.StringHashMapUnmanaged(bool) = .empty;
        for (patch.edits) |edit| if (edit.kind == .distinct) {
            if (try self.get(c.Pair, patch.next.key_template, .distinct, edit.id)) |pair| for ([_][]const u8{ pair.from, pair.to }) |id| if (!endpoints.contains(id)) try endpoints.put(self.a, id, try self.ownPin(patch.next.key_template, id));
            if (edit.value) |value| {
                const pair = try parse(c.Pair, self.a, try encode(self.a, value));
                for ([_][]const u8{ pair.from, pair.to }) |id| if (!endpoints.contains(id)) try endpoints.put(self.a, id, try self.ownPin(patch.next.key_template, id));
            }
        };
        // Detach all replaced edges before attaching any final edge. This
        // keeps count propagation acyclic even when edits rotate a whole path.
        for (patch.edits) |edit| if (edit.kind == .redirect) {
            try detached.put(self.a, edit.id, {});
            if (try self.get(resolver.KeyAlias, patch.next.key_template, .redirect, edit.id)) |edge| {
                const amount = try self.pinCount(patch.next.key_template, edit.id);
                if (amount != 0) {
                    try self.pinChild(patch.next.key_template, edge.target, edit.id, false);
                    try self.movePins(patch.next.key_template, edge.target, amount, false, detached);
                }
            }
        };
        var h = patch.next;
        h.bytes = if (previous) |old| old.bytes else 0;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (patch.edits) |edit| {
            const path = try self.record(h.key_template, edit.kind, edit.id);
            if ((try seen.getOrPut(self.a, path)).found_existing) return error.InvalidResolverCurationRequest;
            if (try self.backend.get(self.backend.ptr, self.a, path)) |old| {
                h.bytes = try std.math.sub(u64, h.bytes, old.len);
                try self.indexRecord(h.key_template, edit.kind, edit.id, old, false);
            }
            if (edit.value) |value_json| {
                const raw = try encode(self.a, value_json);
                h.bytes = try std.math.add(u64, h.bytes, raw.len);
                if (self.max_namespace_bytes) |maximum| if (h.bytes > maximum) return error.ResolverCurationLimitExceeded;
                try self.backend.put(self.backend.ptr, self.a, path, raw);
                try self.indexRecord(h.key_template, edit.kind, edit.id, raw, true);
            } else try self.backend.remove(self.backend.ptr, self.a, path);
        }
        var pin_ready = try parse(PinReady, self.a, (try self.backend.get(self.backend.ptr, self.a, try self.pinReadyPath(h.key_template))).?);
        var changed_endpoints = endpoints.iterator();
        while (changed_endpoints.next()) |entry| {
            const now = try self.ownPin(h.key_template, entry.key_ptr.*);
            if (now != entry.value_ptr.*) {
                try self.movePins(h.key_template, entry.key_ptr.*, 1, now, detached);
                pin_ready.endpoints = if (now) try std.math.add(u64, pin_ready.endpoints, 1) else try std.math.sub(u64, pin_ready.endpoints, 1);
            }
        }
        if (endpoints.count() != 0) try self.backend.put(self.backend.ptr, self.a, try self.pinReadyPath(h.key_template), try encode(self.a, pin_ready));
        for (patch.edits) |edit| if (edit.kind == .redirect) {
            _ = detached.remove(edit.id);
            if (try self.get(resolver.KeyAlias, h.key_template, .redirect, edit.id)) |edge| {
                const count = try self.pinCount(h.key_template, edit.id);
                if (count != 0) try self.pinChild(h.key_template, edge.target, edit.id, true);
                try self.movePins(h.key_template, edge.target, count, true, detached);
            }
        };
        std.crypto.hash.sha2.Sha256.hash(try encode(self.a, .{ if (previous) |old| old.fingerprint else @as([32]u8, @splat(0)), h.key_template, h.revision, h.min_text_chars, h.adoption_revision, h.adoption_standalone, patch.edits }), &h.fingerprint, .{});
        if (previous == null) try self.indexHead(h);
        try self.backend.put(self.backend.ptr, self.a, try self.headPath(h.key_template), try encode(self.a, h));
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
    head: ?Head = null,
    seen: std.StringHashMapUnmanaged([]const u8) = .empty,
    values: Store.Values = .{},
    pub fn add(self: *QueryPolicyUnion, a: A, scratch: A, part: Snapshot) !void {
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
        return self.values.snapshot(self.head orelse return error.InvalidResolverCurationRequest);
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
    var parts: [5][]const u8 = undefined;
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
        var it = self.rows.iterator();
        while (it.next()) |v| if (std.mem.startsWith(u8, v.key_ptr.*, prefix) and (after == null or std.mem.order(u8, v.key_ptr.*, after.?) == .gt)) {
            try rows.append(alloc, .{ .key = v.key_ptr.*, .value = v.value_ptr.* });
        };
        std.mem.sort(Row, rows.items, {}, struct {
            fn less(_: void, l: Row, r: Row) bool {
                return std.mem.lessThan(u8, l.key, r.key);
            }
        }.less);
        const count = @min(limit, rows.items.len);
        self.visited += count;
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

test "resolver curation query policy unions bounded batches with a head lineage fence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const h: Head = .{ .key_template = "entity/{{_key}}", .revision = 7, .min_text_chars = 2, .fingerprint = @splat(1) };
    const part: Snapshot = .{ .head = h, .namespace = .{ .key_template = h.key_template, .revision = 7, .merges = &.{.{ .key = "entity/a", .target = "entity/b" }}, .state = .{ .suppress = .{ .texts = &.{"noise"} } } } };
    const joined = try unionQueryPolicies(a, &.{ part, part });
    try std.testing.expectEqual(@as(usize, 1), joined.namespace.merges.len);
    try std.testing.expectEqual(@as(usize, 1), joined.namespace.state.suppress.texts.len);
    try std.testing.expectEqual(@as(u32, 2), joined.namespace.state.suppress.min_text_chars);
    var divergent = part;
    divergent.head.fingerprint = @splat(2);
    try std.testing.expectError(error.ResolverCurationConflict, unionQueryPolicies(a, &.{ part, divergent }));
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
