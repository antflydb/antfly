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

//! Lite adapter for the shared native destination-owned resolver authority.
//! Admission holds the connection owner fence; inference is exclusively replay.
const std = @import("std");
const h = @import("handles.zig");
const tables = @import("tables.zig");
const native = @import("../storage/db/resolver_authority.zig");
const policy = @import("../storage/db/resolver_curation.zig");
const catalog = @import("../storage/db/catalog/resolver_catalog.zig");
const docstore = @import("../storage/docstore.zig");
const c = @import("antfly_resolver").curation;
const A = std.mem.Allocator;
const Bridge = native.Transaction(docstore.DocStore.Txn);
const prefix = native.lite_prefix;
const marker = native.lite_marker;
const AbsorbedMap = std.StringHashMapUnmanaged([]const c.AbsorbedResult);
const Consumer = struct { db: *h.db_mod.DB, source: []const u8, cfg: h.db_mod.ResolverConfig, synthetic: bool = false };
pub const Input = struct { action: enum { redirects, merge, unmerge, suppress, unsuppress, distinct }, into: ?[]const u8 = null, replace: bool = false, pin_distinct: bool = false, absorbed: c.Absorbed = .reject, actor: []const u8 = "", reason: []const u8 = "", expected_revision: ?u64 = null, wait: ?c.Wait = null };
pub const Operation = enum { list, get, create, decide };
fn json(a: A, item: anytype) ![]u8 {
    return std.json.Stringify.valueAlloc(a, item, .{});
}
fn value(a: A, object: anytype) !std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, a, try json(a, object), .{});
}
pub fn enabled(db: *h.db_mod.DB, a: A) !bool {
    _ = db.core.store.get(a, marker) catch |err| return if (err == error.NotFound) false else err;
    return true;
}
fn consumers(root: *h.Handle, a: A, table: []const u8, key: []const u8) ![]const Consumer {
    var sources: std.ArrayList(struct { db: *h.db_mod.DB, name: []const u8 }) = .empty;
    try sources.append(a, .{ .db = &root.db, .name = "default" });
    var it = root.embedded_tables.iterator();
    while (it.next()) |entry| try sources.append(a, .{ .db = &entry.value_ptr.*.db, .name = entry.key_ptr.* });
    var result: std.ArrayList(Consumer) = .empty;
    for (sources.items) |source| {
        const configs = try source.db.core.index_manager.listResolverStaticDescriptors(a);
        for (configs) |cfg| if (std.mem.eql(u8, cfg.table, table) and policy.keyInNamespace(cfg.key_template, key)) try result.append(a, .{ .db = source.db, .source = source.name, .cfg = cfg });
    }
    return result.items;
}
fn ownedConsumers(a: A, store: native.Store, destination: *h.db_mod.DB, current: []const Consumer, table: []const u8, key: []const u8) ![]const Consumer {
    var out: std.ArrayList(Consumer) = .empty;
    try out.appendSlice(a, current);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (current) |source| try seen.put(a, source.cfg.key_template, {});
    for (try store.heads()) |head| {
        if (seen.contains(head.key_template) or !policy.keyInNamespace(head.key_template, key)) continue;
        try out.append(a, .{ .db = destination, .source = "", .synthetic = true, .cfg = .{ .name = "native_authority", .table = table, .key_template = head.key_template, .source_artifact = "authority_source", .resolution_artifact = "authority_resolution", .promote = false } });
    }
    return out.items;
}
fn initialize(a: A, bridge: *Bridge, store: native.Store, legacy: c.State) !void {
    if (try bridge.backendGet(a, marker) != null) return;
    if (legacy.proposals.len != 0 or legacy.receipt_scope != null) try store.applyQueue(.{ .expected_revision = 0, .revision = 1, .receipt_scope = legacy.receipt_scope, .proposals = legacy.proposals });
    try bridge.backendPut(a, marker, "1");
    // Older binaries must refuse to open after native authority is published.
    try bridge.backendPut(a, catalog.minimum_reader_key, try catalog.readerVersionBytes(catalog.native_resolver_records_reader_version));
}
fn adopt(a: A, store: native.Store, selected: []const Consumer) !void {
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (selected) |consumer| {
        const template = consumer.cfg.key_template;
        if ((try seen.getOrPut(a, template)).found_existing or try store.head(template) != null) continue;
        var namespace: policy.Namespace = .{ .key_template = template };
        for (selected) |source| if (std.mem.eql(u8, source.cfg.key_template, template)) {
            // Full policy is copied once, at adoption, never on warm reads.
            const configs = try source.db.core.index_manager.listResolverIdentities(a);
            for (configs) |cfg| if (std.mem.eql(u8, cfg.name, source.cfg.name)) {
                namespace = try policy.foldNamespace(a, namespace, cfg);
                break;
            };
        };
        try store.adoptNamespace(namespace);
    }
}
fn inspectionScope(a: A, store: native.Store, selected: []const Consumer, source: Consumer, key: []const u8) !native.Snapshot {
    if (try store.component(source.cfg.key_template, &.{key})) |snapshot| return snapshot;
    // Read-only legacy inspection folds only this namespace. Its first
    // authorized mutation publishes the native records.
    var namespace: policy.Namespace = .{ .key_template = source.cfg.key_template };
    for (selected) |consumer| {
        if (consumer.synthetic or !std.mem.eql(u8, consumer.cfg.key_template, source.cfg.key_template)) continue;
        const configs = try consumer.db.core.index_manager.listResolverIdentities(a);
        for (configs) |cfg| if (std.mem.eql(u8, cfg.name, consumer.cfg.name)) {
            namespace = try policy.foldNamespace(a, namespace, cfg);
            break;
        };
    }
    return .{ .head = .{ .key_template = namespace.key_template, .revision = namespace.revision }, .namespace = namespace };
}
fn profileScope(a: A, store: native.Store, selected: []const Consumer, template: []const u8, key: []const u8, into: ?[]const u8) !native.Snapshot {
    if (into) |target| if (!policy.keyInNamespace(template, target)) return error.InvalidResolverCurationRequest;
    var static_aliases: std.ArrayList(@import("antfly_resolver").KeyAlias) = .empty;
    for (selected) |source| if (std.mem.eql(u8, source.cfg.key_template, template)) try static_aliases.appendSlice(a, try source.cfg.effectiveAliasesAlloc(a));
    return (try store.componentWithAliases(template, if (into) |target| &.{ key, target } else &.{key}, static_aliases.items)) orelse error.ResolverNotFound;
}
fn aliases(a: A, cfg: h.db_mod.ResolverConfig, namespace: policy.Namespace) ![]const @import("antfly_resolver").KeyAlias {
    var projected = cfg;
    projected.merges = namespace.merges;
    projected.curation_json = try json(a, namespace.state);
    return projected.effectiveAliasesAlloc(a);
}
fn validate(a: A, selected: []const Consumer, namespace: policy.Namespace) !void {
    for (selected) |source| if (std.mem.eql(u8, source.cfg.key_template, namespace.key_template)) {
        var cfg = source.cfg;
        cfg.merges = namespace.merges;
        cfg.curation_json = try json(a, namespace.state);
        cfg.curation_revision = namespace.revision;
        try cfg.validate();
        _ = try cfg.effectiveAliasesAlloc(a);
    };
}
fn queueScope(a: A, store: native.Store, namespace: policy.Namespace, key: []const u8, into: ?[]const u8) !c.State {
    var keys: std.ArrayList([]const u8) = .empty;
    try keys.append(a, key);
    if (into) |target| try keys.append(a, target);
    for (namespace.merges) |alias| {
        try keys.append(a, alias.key);
        try keys.append(a, alias.target);
    }
    try keys.appendSlice(a, namespace.state.suppress.keys);
    return store.proposalsForKeys(keys.items);
}
fn publishPlan(a: A, store: native.Store, selected: []const Consumer, before: native.Snapshot, plan: c.Plan) !policy.Namespace {
    if (!plan.changed) return before.namespace;
    var next = before.namespace;
    next.revision = try std.math.add(u64, next.revision, 1);
    next.merges = plan.merges;
    next.state = plan.state;
    next.state.proposals = &.{};
    next.state.receipt_scope = null;
    try validate(a, selected, next);
    const patch = try native.difference(a, before.namespace, next);
    if (patch.edits.len == 0 and patch.next.min_text_chars == before.head.min_text_chars) return before.namespace;
    try store.apply(patch);
    return next;
}
fn publishQueue(a: A, store: native.Store, before: c.State, next: c.State) !void {
    var changed: std.ArrayList(c.Proposal) = .empty;
    var old: std.StringHashMapUnmanaged(c.Proposal) = .empty;
    for (before.proposals) |proposal| try old.put(a, proposal.id, proposal);
    for (next.proposals) |proposal| {
        if (old.get(proposal.id)) |previous| if (std.mem.eql(u8, try json(a, previous), try json(a, proposal))) continue;
        try changed.append(a, proposal);
    }
    if (changed.items.len == 0) return;
    const head = try store.queueHead();
    try store.applyQueue(.{ .expected_revision = head.revision, .revision = try std.math.add(u64, head.revision, 1), .receipt_scope = next.receipt_scope, .proposals = changed.items });
}
fn results(a: A, selected: []const Consumer, store: native.Store, key: []const u8, redirects: bool, changed: bool, absorbed: AbsorbedMap) !std.json.Value {
    var rows = std.json.Array.init(a);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var heads: std.StringHashMapUnmanaged(native.Head) = .empty;
    for (selected) |source| {
        if (redirects) {
            if ((try seen.getOrPut(a, source.cfg.key_template)).found_existing) continue;
            const snapshot = try inspectionScope(a, store, selected, source, key);
            const scoped = try c.entityState(a, snapshot.namespace.state, snapshot.namespace.merges, key);
            try rows.append(try value(a, .{ .source_table = source.cfg.table, .resolver = "", .key_template = source.cfg.key_template, .revision = snapshot.head.revision, .redirects = try c.redirectsWithOrigins(a, scoped.redirects, scoped.curation, snapshot.namespace.merges), .canonical_key = scoped.canonical_key, .curation = scoped.curation }));
        } else {
            if (source.synthetic) continue;
            const slot = try heads.getOrPut(a, source.cfg.key_template);
            if (!slot.found_existing) slot.value_ptr.* = (try store.head(source.cfg.key_template)) orelse return error.ResolverNotFound;
            try rows.append(try value(a, .{ .source_table = source.source, .resolver = source.cfg.name, .revision = slot.value_ptr.revision, .accepted = true, .changed = changed, .absorbed = absorbed.get(source.cfg.key_template) orelse &.{}, .applied = source.cfg.namespace_curation_generation >= slot.value_ptr.revision and try policy.applied(source.db, source.cfg), .replay_sequence = source.db.core.store.lastReplaySequence(0), .replay_sequence_kind = "source_journal" }));
        }
    }
    return .{ .array = rows };
}
fn finish(a: A, bridge: *Bridge, txn: *docstore.DocStore.Txn, db: *h.db_mod.DB, lease: anytype) !void {
    try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
    try bridge.flush(a);
    try txn.commit();
    db.core.index_manager.minimum_reader_version.store(catalog.native_resolver_records_reader_version, .release);
}
pub fn entity(root: *h.Handle, a: A, table: []const u8, key: []const u8, input: Input, legacy: c.State) ![]const u8 {
    const db = try tables.get(root, table);
    const reading = input.action == .redirects;
    var lease = if (reading) try db.local_execution.row_policy_gate.enterRawRead() else try db.local_execution.row_policy_gate.enterRaw();
    defer lease.release();
    if (!reading and try db.maintenanceRequiresOrderedApply()) return error.OrderedResolverReviewRequired;
    var selected: []const Consumer = &.{};
    if (!reading) {
        try tables.load(root);
        selected = try consumers(root, a, table, key);
    }
    var txn = if (reading) try db.core.store.beginReadTxn() else try db.core.store.beginWriteTxn();
    var txn_live = true;
    defer if (txn_live) txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const store = bridge.store(a, prefix);
    if (!reading) {
        try initialize(a, &bridge, store, legacy);
        try adopt(a, store, selected);
    }
    selected = try ownedConsumers(a, store, db, selected, table, key);
    if (reading and selected.len == 0) {
        txn.abort();
        txn_live = false;
        try tables.load(root);
        txn = try db.core.store.beginReadTxn();
        txn_live = true;
        selected = try consumers(root, a, table, key);
    }
    if (selected.len == 0) return error.ResolverNotFound;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var changed = false;
    var absorbed: AbsorbedMap = .empty;
    for (selected) |source| {
        if ((try seen.getOrPut(a, source.cfg.key_template)).found_existing) continue;
        if (input.action == .redirects) continue;
        const before = try profileScope(a, store, selected, source.cfg.key_template, key, input.into);
        if (input.expected_revision) |expected| if (expected != before.head.revision) return error.ResolverCurationConflict;
        const action: c.Action = switch (input.action) {
            .redirects => unreachable,
            .merge => .merge,
            .unmerge => .unmerge,
            .suppress => .suppress,
            .unsuppress => .unsuppress,
            .distinct => .distinct,
        };
        const request: c.Request = .{ .action = action, .key = key, .to = input.into, .replace = input.replace, .pin_distinct = input.pin_distinct, .absorbed = input.absorbed, .actor = input.actor, .reason = input.reason, .expected_revision = before.head.revision, .wait = input.wait };
        const stamp = policy.manualOrigin(request, try std.math.add(u64, before.head.revision, 1));
        const queue = try queueScope(a, store, before.namespace, key, input.into);
        const state = c.bindReceiptScope(before.namespace.state, queue.receipt_scope);
        var plan = try c.planWithOrigin(a, try json(a, state), before.namespace.merges, try aliases(a, source.cfg, before.namespace), request, stamp);
        _ = try c.recordSupersessions(a, &plan, queue.proposals, request, stamp);
        _ = try publishPlan(a, store, selected, before, plan);
        try absorbed.put(a, source.cfg.key_template, plan.absorbed);
        var next_queue = queue;
        next_queue.proposals = try c.projectReceipts(a, queue.proposals, plan.state.proposals);
        try publishQueue(a, store, queue, next_queue);
        changed = changed or plan.changed;
    }
    const rows = try results(a, selected, store, key, input.action == .redirects, changed, absorbed);
    if (!reading) {
        try finish(a, &bridge, &txn, db, &lease);
        txn_live = false;
    } else try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
    return json(a, .{ .entity_table = table, .key = key, .atomicity = "namespace", .complete = true, .results = rows });
}
/// Read/write queues use the same bounded indexes as routed admission.
pub fn proposals(root: *h.Handle, a: A, operation: Operation, table: []const u8, id: []const u8, raw: []const u8, legacy: c.State) ![]const u8 {
    const db = try tables.get(root, table);
    const reading = operation == .list or operation == .get;
    var lease = if (reading) try db.local_execution.row_policy_gate.enterRawRead() else try db.local_execution.row_policy_gate.enterRaw();
    defer lease.release();
    if (!reading and try db.maintenanceRequiresOrderedApply()) return error.OrderedResolverReviewRequired;
    var txn = if (reading) try db.core.store.beginReadTxn() else try db.core.store.beginWriteTxn();
    var txn_live = true;
    defer if (txn_live) txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const store = bridge.store(a, prefix);
    if (!reading) try initialize(a, &bridge, store, legacy);
    if (operation == .list) {
        const Query = struct { state: enum { open, accepted, rejected, superseded, all } = .open, action: ?c.EntityAction = null, cursor: []const u8 = "", limit: u16 = 64 };
        const query = try std.json.parseFromSliceLeaky(Query, a, if (raw.len == 0) "{}" else raw, .{});
        const parsed = try c.proposalQuery(a, try std.fmt.allocPrint(a, "state={s}&cursor={s}&limit={d}{s}", .{ @tagName(query.state), query.cursor, query.limit, if (query.action) |action| try std.fmt.allocPrint(a, "&action={s}", .{@tagName(action)}) else "" }));
        const page = try store.proposalPage(parsed.state, parsed.action, parsed.cursor, parsed.limit);
        try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
        return json(a, page);
    }
    const creation = if (operation == .create) try std.json.parseFromSliceLeaky(c.ProposalRequest, a, raw, .{}) else null;
    const proposal_id = if (creation) |request| try c.proposalId(a, request) else id;
    var existing = try store.proposal(proposal_id);
    if (operation == .get or (operation == .create and existing != null)) {
        const proposal = existing orelse return error.NotFound;
        try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
        return json(a, proposal);
    }
    const key = if (existing) |proposal| proposal.key else if (creation) |request| request.key else return error.NotFound;
    const selected = try ownedConsumers(a, store, db, try consumers(root, a, table, key), table, key);
    if (selected.len == 0 and creation != null) return error.ResolverNotFound;
    if (creation) |request| if (request.into) |target| for (selected) |source| if (!policy.keyInNamespace(source.cfg.key_template, target)) return error.InvalidResolverCurationRequest;
    if (operation == .decide and existing.?.state != .open) {
        const proposal = existing.?;
        const decision = try std.json.parseFromSliceLeaky(c.ProposalDecision, a, raw, .{});
        if (decision.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
        if (!decision.accept and (decision.options != null or (decision.pin_distinct and proposal.action != .merge))) return error.InvalidResolverCurationRequest;
        const wanted: c.ProposalState = if (decision.accept) .accepted else .rejected;
        if (proposal.state != wanted) return error.ResolverCurationConflict;
        var consumers_with_heads: std.ArrayList(Consumer) = .empty;
        for (selected) |source| if (try store.head(source.cfg.key_template) != null) try consumers_with_heads.append(a, source);
        const rows = try results(a, consumers_with_heads.items, store, key, false, false, .empty);
        const applied = if (!decision.accept and !decision.pin_distinct) true else for (rows.array.items) |row| {
            if (!row.object.get("applied").?.bool) break false;
        } else true;
        try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
        return json(a, .{ .proposal = proposal, .atomicity = "namespace", .changed = false, .complete = true, .applied = applied, .results = rows });
    }
    try adopt(a, store, selected);
    var head = try store.queueHead();
    if (creation) |request| {
        if (head.receipt_scope == null) {
            var entropy: [16]u8 = undefined;
            try @import("antfly_platform").entropy.fill(h.handleLockIo(), &entropy);
            head.receipt_scope = std.fmt.bytesToHex(entropy, .lower);
        }
        const plan = try c.propose(a, .{ .receipt_scope = head.receipt_scope }, &.{}, request, policy.manualOrigin(.{ .action = .suppress, .actor = request.actor, .reason = request.reason }, 0));
        try publishQueue(a, store, .{}, plan.state);
        existing = plan.state.proposals[0];
        try finish(a, &bridge, &txn, db, &lease);
        txn_live = false;
        return json(a, existing.?);
    }
    const proposal = existing.?;
    const decision = try std.json.parseFromSliceLeaky(c.ProposalDecision, a, raw, .{});
    if (decision.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
    if (!decision.accept and (decision.options != null or (decision.pin_distinct and proposal.action != .merge))) return error.InvalidResolverCurationRequest;
    const wanted: c.ProposalState = if (decision.accept) .accepted else .rejected;
    if (proposal.state != .open and proposal.state != wanted) return error.ResolverCurationConflict;
    if (proposal.state == .open and selected.len == 0 and (decision.accept or decision.pin_distinct)) return error.ResolverNotFound;
    var queue_before: std.StringHashMapUnmanaged(c.Proposal) = .empty;
    var queue_after: std.StringHashMapUnmanaged(c.Proposal) = .empty;
    var decision_origin = policy.manualOrigin(.{ .action = .suppress, .actor = decision.actor, .reason = decision.reason }, 0);
    decision_origin.origin = .review;
    decision_origin.proposal_id = proposal.id;
    var changed = false;
    var absorbed: AbsorbedMap = .empty;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    if (proposal.state == .open) for (selected) |source| {
        if ((try seen.getOrPut(a, source.cfg.key_template)).found_existing) continue;
        const before = try profileScope(a, store, selected, source.cfg.key_template, key, proposal.into);
        if (decision.expected_revision) |expected| if (expected != before.head.revision) return error.ResolverCurationConflict;
        var queue = try queueScope(a, store, before.namespace, key, proposal.into);
        var members: std.ArrayList(c.Proposal) = .empty;
        for (queue.proposals) |member| if (!std.mem.eql(u8, member.id, proposal.id)) try members.append(a, member);
        try members.append(a, proposal);
        queue.proposals = members.items;
        var state = c.bindReceiptScope(before.namespace.state, queue.receipt_scope);
        state.proposals = queue.proposals;
        const origin = policy.manualOrigin(.{ .action = .suppress, .actor = decision.actor, .reason = decision.reason }, try std.math.add(u64, before.head.revision, 1));
        var plan = try c.decide(a, state, before.namespace.merges, try aliases(a, source.cfg, before.namespace), proposal.id, decision, origin);
        var review_origin = origin;
        review_origin.origin = .review;
        review_origin.proposal_id = proposal.id;
        _ = try c.recordSupersessions(a, &plan, queue.proposals, c.decisionRequest(proposal, decision), review_origin);
        _ = try publishPlan(a, store, selected, before, plan);
        try absorbed.put(a, source.cfg.key_template, plan.absorbed);
        for (queue.proposals) |member| if (!(try queue_before.getOrPut(a, member.id)).found_existing) try queue_before.put(a, member.id, member);
        for (try c.projectReceipts(a, queue.proposals, plan.state.proposals)) |member| {
            var record = member;
            if (std.mem.eql(u8, record.id, proposal.id)) {
                record.state = wanted;
                record.decided = decision_origin;
            }
            const slot = try queue_after.getOrPut(a, record.id);
            if (!slot.found_existing or slot.value_ptr.state == .open) slot.value_ptr.* = record;
        }
        changed = changed or plan.changed;
    };
    if (queue_after.count() != 0) {
        var previous: std.ArrayList(c.Proposal) = .empty;
        var next: std.ArrayList(c.Proposal) = .empty;
        var before_values = queue_before.valueIterator();
        while (before_values.next()) |record| try previous.append(a, record.*);
        var after_values = queue_after.valueIterator();
        while (after_values.next()) |record| try next.append(a, record.*);
        try publishQueue(a, store, .{ .proposals = previous.items }, .{ .receipt_scope = head.receipt_scope, .proposals = next.items });
    }
    var decided = (try store.proposal(proposal.id)) orelse proposal;
    if (decided.state == .open) {
        decided.state = wanted;
        decided.decided = decision_origin;
        try publishQueue(a, store, .{ .proposals = &.{proposal} }, .{ .receipt_scope = head.receipt_scope, .proposals = &.{decided} });
    }
    const rows = try results(a, selected, store, key, false, changed, absorbed);
    try finish(a, &bridge, &txn, db, &lease);
    txn_live = false;
    const applied = if (!decision.accept and !decision.pin_distinct) true else for (rows.array.items) |row| {
        if (!row.object.get("applied").?.bool) break false;
    } else true;
    return json(a, .{ .proposal = decided, .atomicity = "namespace", .changed = changed, .complete = true, .applied = applied, .results = if (!decision.accept and !decision.pin_distinct) std.json.Value{ .array = std.json.Array.init(a) } else rows });
}
/// A source pulls at most one current namespace projection per maintenance
/// turn. Called on the detached owner snapshot, outside the writer path lease.
pub fn pullProjection(root: *h.Handle, database: *h.db_mod.DB, a: A) !bool {
    const configs = try database.core.index_manager.listResolverStaticDescriptors(a);
    for (configs) |cfg| {
        const destination = tables.get(root, cfg.table) catch |err| switch (err) {
            error.UndefinedTable => continue,
            else => return err,
        };
        var txn = try destination.core.store.beginReadTxn();
        var snapshot: native.Snapshot = undefined;
        {
            defer txn.abort();
            var bridge: Bridge = .{ .txn = &txn };
            const store = bridge.store(a, prefix);
            const head = try store.head(cfg.key_template) orelse continue;
            if (cfg.namespace_curation_generation == head.revision and cfg.curation_revision == head.revision) continue;
            if (cfg.namespace_curation_generation > head.revision) return error.ResolverCurationConflict;
            snapshot = (try store.exportNamespace(cfg.key_template)) orelse continue;
        }
        const head = snapshot.head;
        var next = cfg;
        var state = snapshot.namespace.state;
        state.receipt_scope = (try queueHead(destination, a)).receipt_scope;
        next.curation_json = try json(a, state);
        next.curation_revision = head.revision;
        next.namespace_curation_generation = head.revision;
        next.merges = snapshot.namespace.merges;
        next.merge_generation = try std.math.add(u64, cfg.effectiveGeneration(), 1);
        next.merge_state_version = if (next.merges.len == 0) 2 else 1;
        try next.validate();
        if (root.maintenance_publication) |publication| publication.resolver_dependency = .{ .table = cfg.table, .template = cfg.key_template, .table_id = destination.core.identity_namespace.table_id, .revision = head.revision };
        defer if (root.maintenance_publication) |publication| {
            publication.resolver_dependency = null;
        };
        _ = try database.upsertResolverWithResultOptions(next, .{ .drain_backfill = false, .wait_for_activity = true });
        return true;
    }
    return false;
}
pub fn revision(db: *h.db_mod.DB, alloc: A, template: []const u8) !?u64 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    return if (try bridge.store(arena.allocator(), prefix).head(template)) |head| head.revision else null;
}
pub fn queueHead(db: *h.db_mod.DB, a: A) !native.QueueHead {
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    return bridge.store(a, prefix).queueHead();
}
pub fn owns(db: *h.db_mod.DB, a: A, template: []const u8) !bool {
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    return bridge.store(a, prefix).ownsKeyTemplate(template);
}
pub fn hasProjectionDebt(root: *h.Handle, database: *h.db_mod.DB, a: A) !bool {
    const configs = try database.core.index_manager.listResolverStaticDescriptors(a);
    for (configs) |cfg| {
        const destination = tables.get(root, cfg.table) catch |err| switch (err) {
            error.UndefinedTable => continue,
            else => return err,
        };
        var txn = try destination.core.store.beginReadTxn();
        defer txn.abort();
        var bridge: Bridge = .{ .txn = &txn };
        const head = try bridge.store(a, prefix).head(cfg.key_template) orelse continue;
        if (cfg.namespace_curation_generation != head.revision or cfg.curation_revision != head.revision) return true;
    }
    return false;
}
