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
fn ensureConsumerIndex(root: *h.Handle, a: A) !void {
    const directory = @import("../storage/lite/resolver_consumers.zig");
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical == null) return;
    {
        const ready = blk: {
            var read = try physical.?.beginRead();
            defer read.abort();
            break :blk if (read.get(directory.marker)) |_| true else |err| if (err == error.NotFound) false else return err;
        };
        if (!ready) {
            var write = try physical.?.beginWrite();
            errdefer write.abort();
            try directory.bootstrap(a, &write);
            try write.commit();
            root.db.core.index_manager.minimum_reader_version.store(catalog.resolver_projection_delta_reader_version, .release);
        }
    }
}
fn consumers(root: *h.Handle, a: A, table: []const u8, key: []const u8) ![]const Consumer {
    const directory = @import("../storage/lite/resolver_consumers.zig");
    // Native Lite stores share a physical transaction space. Bootstrap legacy
    // bindings once from catalog bytes; warm calls seek only this destination.
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical == null) return legacyConsumers(root, a, table, key);
    try ensureConsumerIndex(root, a);
    var txn = try physical.?.beginRead();
    defer txn.abort();
    var bindings: std.ArrayList(directory.Binding) = .empty;
    for (try directory.matchingNamespacePrefixes(a, &txn, table, key)) |target| {
        var cursor = try txn.openCursor();
        defer cursor.close();
        var row = try directory.seekEntry(&cursor, target);
        while (row) |entry| {
            if (!std.mem.startsWith(u8, entry.key, target)) break;
            try bindings.append(a, try std.json.parseFromSliceLeaky(directory.Binding, a, entry.value, .{ .allocate = .alloc_always }));
            row = try directory.nextEntry(&cursor);
        }
    }
    try tables.loadCatalog(root);
    var result: std.ArrayList(Consumer) = .empty;
    for (bindings.items) |binding| {
        if (binding.source_id != 0 and root.embedded_table_records.get(binding.source) != binding.source_id) continue;
        const source = try tables.get(root, binding.source);
        const cfg = try source.core.index_manager.resolverStaticDescriptor(a, binding.resolver) orelse continue;
        if (!std.mem.eql(u8, cfg.table, table) or !std.mem.eql(u8, cfg.key_template, binding.key_template)) return error.ResolverCurationConflict;
        try result.append(a, .{ .db = source, .source = binding.source, .cfg = cfg });
    }
    return result.items;
}
fn legacyConsumers(root: *h.Handle, a: A, table: []const u8, key: []const u8) ![]const Consumer {
    try tables.load(root);
    var result: std.ArrayList(Consumer) = .empty;
    var sources = root.embedded_tables.iterator();
    const defaults = try root.db.core.index_manager.listResolverStaticDescriptors(a);
    for (defaults) |cfg| if (std.mem.eql(u8, cfg.table, table) and policy.keyInNamespace(cfg.key_template, key)) try result.append(a, .{ .db = &root.db, .source = "default", .cfg = cfg });
    while (sources.next()) |source| for (try source.value_ptr.*.db.core.index_manager.listResolverStaticDescriptors(a)) |cfg| {
        if (std.mem.eql(u8, cfg.table, table) and policy.keyInNamespace(cfg.key_template, key)) try result.append(a, .{ .db = &source.value_ptr.*.db, .source = source.key_ptr.*, .cfg = cfg });
    };
    return result.items;
}
fn ownedConsumers(a: A, store: native.Store, destination: *h.db_mod.DB, current: []const Consumer, table: []const u8, key: []const u8) ![]const Consumer {
    var out: std.ArrayList(Consumer) = .empty;
    try out.appendSlice(a, current);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (current) |source| try seen.put(a, source.cfg.key_template, {});
    for (try store.matchingHeads(key)) |head| {
        if (seen.contains(head.key_template) or !policy.keyInNamespace(head.key_template, key)) continue;
        try out.append(a, .{ .db = destination, .source = "", .synthetic = true, .cfg = .{ .name = "native_authority", .table = table, .key_template = head.key_template, .source_artifact = "authority_source", .resolution_artifact = "authority_resolution", .promote = false } });
    }
    return out.items;
}
fn initialize(a: A, bridge: *Bridge, store: native.Store, legacy: c.State) !void {
    try bridge.backendPut(a, catalog.minimum_reader_key, try catalog.readerVersionBytes(catalog.resolver_projection_delta_reader_version));
    try store.ensureHeadRoutes();
    if (try bridge.backendGet(a, marker) != null) return;
    if (legacy.proposals.len != 0 or legacy.receipt_scope != null) try store.applyQueue(.{ .expected_revision = 0, .revision = 1, .receipt_scope = legacy.receipt_scope, .proposals = legacy.proposals });
    try bridge.backendPut(a, marker, "1");
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
fn consumerProfiles(a: A, selected: []const Consumer, template: []const u8) ![]const native.AliasProfile {
    var profiles: std.ArrayList(native.AliasProfile) = .empty;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (selected) |source| if (std.mem.eql(u8, source.cfg.key_template, template)) {
        const key = try json(a, .{ source.cfg.aliases, source.cfg.suppress_json });
        if ((try seen.getOrPut(a, key)).found_existing) continue;
        try profiles.append(a, .{ .key_template = template, .aliases = source.cfg.aliases, .suppress_json = source.cfg.suppress_json });
    };
    return profiles.items;
}
fn profileScope(a: A, store: native.Store, selected: []const Consumer, template: []const u8, key: []const u8, into: ?[]const u8) !native.Snapshot {
    if (into) |target| if (!policy.keyInNamespace(template, target)) return error.InvalidResolverCurationRequest;
    var static_aliases: std.ArrayList(@import("antfly_resolver").KeyAlias) = .empty;
    for (try consumerProfiles(a, selected, template)) |profile| try static_aliases.appendSlice(a, profile.aliases);
    return (try store.componentWithAliases(template, if (into) |target| &.{ key, target } else &.{key}, static_aliases.items)) orelse error.ResolverNotFound;
}
fn aliases(a: A, selected: []const Consumer, namespace: policy.Namespace, key: []const u8, into: ?[]const u8) ![]const @import("antfly_resolver").KeyAlias {
    return native.effectiveAliases(a, namespace, try consumerProfiles(a, selected, namespace.key_template), key, into);
}
fn validate(a: A, selected: []const Consumer, namespace: policy.Namespace) !void {
    try native.validateProfiles(a, namespace, try consumerProfiles(a, selected, namespace.key_template));
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
fn results(a: A, authority_table_id: u64, selected: []const Consumer, store: native.Store, key: []const u8, redirects: bool, changed: bool, absorbed: AbsorbedMap) !std.json.Value {
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
            try rows.append(try value(a, .{ .source_table = source.source, .resolver = source.cfg.name, .revision = slot.value_ptr.revision, .accepted = true, .changed = changed, .absorbed = absorbed.get(source.cfg.key_template) orelse &.{}, .applied = !source.cfg.namespace_curation_missing and source.cfg.namespace_curation_table_id == authority_table_id and source.cfg.namespace_curation_generation >= slot.value_ptr.revision and try policy.applied(source.db, source.cfg), .replay_sequence = source.db.core.store.lastReplaySequence(0), .replay_sequence_kind = "source_journal" }));
        }
    }
    return .{ .array = rows };
}
fn finish(a: A, bridge: *Bridge, txn: *docstore.DocStore.Txn, db: *h.db_mod.DB, lease: anytype) !void {
    try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
    try bridge.flush(a);
    try txn.commit();
    db.core.index_manager.minimum_reader_version.store(catalog.resolver_projection_delta_reader_version, .release);
}
pub fn entity(root: *h.Handle, a: A, table: []const u8, key: []const u8, input: Input, legacy: c.State) ![]const u8 {
    const db = try tables.get(root, table);
    const reading = input.action == .redirects;
    var lease = if (reading) try db.local_execution.row_policy_gate.enterRawRead() else try db.local_execution.row_policy_gate.enterRaw();
    defer lease.release();
    if (!reading and try db.maintenanceRequiresOrderedApply()) return error.OrderedResolverReviewRequired;
    var selected: []const Consumer = &.{};
    if (!reading) {
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
        // Consumer discovery may bootstrap its physical reverse index and
        // open matching sources. Do that before pinning the destination read.
        selected = try consumers(root, a, table, key);
        txn = try db.core.store.beginReadTxn();
        txn_live = true;
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
        var plan = try c.planWithOrigin(a, try json(a, state), before.namespace.merges, try native.planningAliases(a, before.namespace, try aliases(a, selected, before.namespace, key, input.into), action, key, input.into), request, stamp);
        _ = try c.recordSupersessions(a, &plan, queue.proposals, request, stamp);
        _ = try publishPlan(a, store, selected, before, plan);
        try absorbed.put(a, source.cfg.key_template, plan.absorbed);
        var next_queue = queue;
        next_queue.proposals = try c.projectReceipts(a, queue.proposals, plan.state.proposals);
        try publishQueue(a, store, queue, next_queue);
        changed = changed or plan.changed;
    }
    const rows = try results(a, db.core.identity_namespace.table_id, selected, store, key, input.action == .redirects, changed, absorbed);
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
    // Open matching sources before the destination write transaction. All
    // namespaces share the native file writer; queue-only decisions need none.
    var selected: []const Consumer = &.{};
    if (!reading) {
        const create = if (operation == .create) try std.json.parseFromSliceLeaky(c.ProposalRequest, a, raw, .{}) else null;
        const decision = if (operation == .decide) try std.json.parseFromSliceLeaky(c.ProposalDecision, a, raw, .{}) else null;
        const pid = if (create) |request| try c.proposalId(a, request) else id;
        var read = try db.core.store.beginReadTxn();
        var probe: Bridge = .{ .txn = &read };
        const existing = try probe.store(a, prefix).proposal(pid);
        const queue_only_receipt = try probe.store(a, prefix).queueOnly(pid);
        read.abort();
        const current = existing orelse for (legacy.proposals) |proposal| {
            if (std.mem.eql(u8, proposal.id, pid)) break proposal;
        } else null;
        const terminal_policy = if (current) |proposal| proposal.state != .open and !(queue_only_receipt or (proposal.state == .rejected and proposal.action != .merge)) else false;
        const need_sources = if (create != null) current == null else decision.?.accept or decision.?.pin_distinct or decision.?.expected_revision != null or terminal_policy;
        if (need_sources) {
            const key = if (current) |proposal| proposal.key else if (create) |request| request.key else return error.NotFound;
            selected = try consumers(root, a, table, key);
        }
    }
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
    if (operation == .decide) {
        const proposal = existing orelse return error.NotFound;
        const decision = try std.json.parseFromSliceLeaky(c.ProposalDecision, a, raw, .{});
        if (decision.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
        const queue_only_receipt = try store.queueOnly(proposal.id) or (proposal.state == .rejected and proposal.action != .merge);
        if (!decision.accept and !decision.pin_distinct and decision.expected_revision == null and (proposal.state == .open or queue_only_receipt)) {
            if (decision.options != null) return error.InvalidResolverCurationRequest;
            if (proposal.state != .open and proposal.state != .rejected) return error.ResolverCurationConflict;
            var decided = proposal;
            const changed = proposal.state == .open;
            if (changed) {
                var origin = policy.manualOrigin(.{ .action = .suppress, .actor = decision.actor, .reason = decision.reason }, 0);
                origin.origin = .review;
                origin.proposal_id = proposal.id;
                decided.state = .rejected;
                decided.decided = origin;
                try publishQueue(a, store, .{ .proposals = &.{proposal} }, .{ .receipt_scope = (try store.queueHead()).receipt_scope, .proposals = &.{decided} });
                try store.markQueueOnly(proposal.id);
                try finish(a, &bridge, &txn, db, &lease);
                txn_live = false;
            } else try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
            return json(a, .{ .proposal = decided, .atomicity = "namespace", .changed = changed, .complete = true, .applied = true, .results = std.json.Value{ .array = std.json.Array.init(a) } });
        }
    }
    const key = if (existing) |proposal| proposal.key else if (creation) |request| request.key else return error.NotFound;
    selected = try ownedConsumers(a, store, db, selected, table, key);
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
        const rows = try results(a, db.core.identity_namespace.table_id, consumers_with_heads.items, store, key, false, false, .empty);
        const applied = if (try store.queueOnly(proposal.id) or (!decision.accept and proposal.action != .merge)) true else for (rows.array.items) |row| {
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
        var plan = try c.decide(a, state, before.namespace.merges, try native.planningAliases(a, before.namespace, try aliases(a, selected, before.namespace, key, proposal.into), c.decisionRequest(proposal, decision).action, key, proposal.into), proposal.id, decision, origin);
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
    const rows = try results(a, db.core.identity_namespace.table_id, selected, store, key, false, changed, absorbed);
    try finish(a, &bridge, &txn, db, &lease);
    txn_live = false;
    const applied = if (!decision.accept and !decision.pin_distinct) true else for (rows.array.items) |row| {
        if (!row.object.get("applied").?.bool) break false;
    } else true;
    return json(a, .{ .proposal = decided, .atomicity = "namespace", .changed = changed, .complete = true, .applied = applied, .results = if (!decision.accept and !decision.pin_distinct) std.json.Value{ .array = std.json.Array.init(a) } else rows });
}
const Projection = struct { destination: ?*h.db_mod.DB, head: ?native.Head, table_id: u64, revision: u64, missing: bool, changed: bool };
fn projection(root: *h.Handle, a: A, cfg: h.db_mod.ResolverConfig) !?Projection {
    const destination = tables.get(root, cfg.table) catch |err| switch (err) {
        error.UndefinedTable => null,
        else => return err,
    };
    if (destination == null) {
        if (cfg.namespace_curation_table_id == 0) return null;
        return .{ .destination = null, .head = null, .table_id = cfg.namespace_curation_table_id, .revision = cfg.namespace_curation_generation, .missing = true, .changed = !cfg.namespace_curation_missing };
    }
    const db = destination.?;
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const head = try bridge.store(a, prefix).head(cfg.key_template);
    const table_id = db.core.identity_namespace.table_id;
    if (head == null and cfg.namespace_curation_table_id == 0) return null;
    if (head == null and cfg.namespace_curation_table_id == table_id and !cfg.namespace_curation_missing and cfg.namespace_curation_generation != 0) return error.InvalidMetadataBatch;
    const projected_revision = if (head) |authority_head| authority_head.revision else 0;
    const missing = head == null;
    return .{ .destination = db, .head = head, .table_id = table_id, .revision = projected_revision, .missing = missing, .changed = cfg.namespace_curation_table_id != table_id or cfg.namespace_curation_generation != projected_revision or cfg.namespace_curation_missing != missing };
}
/// Build one current namespace outside writer leases and publish all local
/// consumers atomically. Missing/recreated destinations retract old projections
/// immediately, independent of physical cleanup of the retired authority.
pub fn pullProjection(root: *h.Handle, database: *h.db_mod.DB, a: A) !bool {
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical == null) return legacyPullProjection(root, database, a);
    try ensureConsumerIndex(root, a);
    const jobs = @import("../storage/lite/resolver_consumers.zig");
    var txn = try database.core.store.beginReadTxn();
    var live = true;
    defer if (live) txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const after = try bridge.backendGet(a, jobs.cursor_key);
    var page = try bridge.store(a, "").backend.scan(&bridge, a, jobs.work_prefix, after, 1);
    if (page.rows.len == 0 and after != null) page = try bridge.store(a, "").backend.scan(&bridge, a, jobs.work_prefix, null, 1);
    if (page.rows.len == 0) return false;
    const row = page.rows[0];
    const work = try std.json.parseFromSliceLeaky(jobs.Work, a, row.value, .{});
    try jobs.validateWork(a, row.key, work);
    txn.abort();
    live = false;
    const observation = (try database.core.index_manager.resolverNamespaceObservation(a, work.table, work.key_template)) orelse {
        try database.completeResolverProjectionWork(row.key);
        return true;
    };
    const cfg: h.db_mod.ResolverConfig = .{ .name = "projection_work", .table = work.table, .key_template = work.key_template, .source_artifact = "source", .resolution_artifact = "resolved", .namespace_curation_table_id = observation.namespace_curation_table_id, .namespace_curation_generation = observation.namespace_curation_generation, .namespace_curation_missing = observation.namespace_curation_missing };
    const desired = (try projection(root, a, cfg)) orelse {
        // Empty work is still a publication: a concurrent destination creation
        // or authority adoption must leave its newly queued work outstanding.
        const destination = tables.get(root, cfg.table) catch |err| switch (err) {
            error.UndefinedTable => null,
            else => return err,
        };
        if (root.maintenance_publication) |publication| publication.resolver_dependency = .{ .table = cfg.table, .template = cfg.key_template, .table_id = if (destination) |db| db.core.identity_namespace.table_id else null, .head = null };
        defer if (root.maintenance_publication) |publication| {
            publication.resolver_dependency = null;
        };
        try database.completeResolverProjectionWork(row.key);
        return true;
    };
    var namespace: policy.Namespace = .{ .key_template = cfg.key_template, .revision = desired.revision };
    if (!desired.missing) {
        var read = try desired.destination.?.core.store.beginReadTxn();
        defer read.abort();
        var source: Bridge = .{ .txn = &read };
        const snapshot = (try source.store(a, prefix).exportNamespace(cfg.key_template)) orelse return error.EnrichmentSourceChanged;
        if (snapshot.head.revision != desired.revision or !std.mem.eql(u8, &snapshot.head.fingerprint, &desired.head.?.fingerprint)) return error.EnrichmentSourceChanged;
        namespace = snapshot.namespace;
        namespace.state.receipt_scope = (try queueHead(desired.destination.?, a)).receipt_scope;
    }
    if (root.maintenance_publication) |publication| publication.resolver_dependency = .{ .table = cfg.table, .template = cfg.key_template, .table_id = if (desired.destination != null) desired.table_id else null, .head = if (desired.head) |head| .{ .revision = head.revision, .fingerprint = head.fingerprint } else null };
    defer if (root.maintenance_publication) |publication| {
        publication.resolver_dependency = null;
    };
    _ = try database.publishResolverNamespaceProjectionWork(cfg.table, desired.table_id, namespace, .{ .key = row.key });
    return true;
}
fn legacyPullProjection(root: *h.Handle, database: *h.db_mod.DB, a: A) !bool {
    const configs = try database.core.index_manager.listResolverStaticDescriptors(a);
    for (configs) |cfg| {
        const desired = (try projection(root, a, cfg)) orelse continue;
        if (!desired.changed) continue;
        var namespace: policy.Namespace = .{ .key_template = cfg.key_template, .revision = desired.revision };
        if (!desired.missing) {
            var txn = try desired.destination.?.core.store.beginReadTxn();
            defer txn.abort();
            var bridge: Bridge = .{ .txn = &txn };
            const snapshot = (try bridge.store(a, prefix).exportNamespace(cfg.key_template)) orelse return error.EnrichmentSourceChanged;
            if (snapshot.head.revision != desired.revision or !std.mem.eql(u8, &snapshot.head.fingerprint, &desired.head.?.fingerprint)) return error.EnrichmentSourceChanged;
            namespace = snapshot.namespace;
            namespace.state.receipt_scope = (try queueHead(desired.destination.?, a)).receipt_scope;
        }
        if (root.maintenance_publication) |publication| publication.resolver_dependency = .{ .table = cfg.table, .template = cfg.key_template, .table_id = if (desired.destination != null) desired.table_id else null, .head = if (desired.head) |authority_head| .{ .revision = authority_head.revision, .fingerprint = authority_head.fingerprint } else null };
        defer if (root.maintenance_publication) |publication| {
            publication.resolver_dependency = null;
        };
        _ = try database.publishResolverNamespaceProjection(cfg.table, desired.table_id, namespace);
        return true;
    }
    return false;
}
pub fn listCurrentPolicy(root: *h.Handle, database: *h.db_mod.DB, a: A, name: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var items = std.json.Array.init(scratch);
    for (try database.core.index_manager.listResolverStaticDescriptors(scratch)) |cfg| {
        if (name.len != 0 and !std.mem.eql(u8, cfg.name, name)) continue;
        const current = try currentConfig(root, database, scratch, cfg, null);
        const item = try policy.inspectionValue(database, scratch, current);
        if (name.len != 0) return json(a, item);
        try items.append(item);
    }
    if (name.len != 0) return error.ResolverNotFound;
    return json(a, std.json.Value{ .array = items });
}

pub const MentionPolicy = struct { key: []const u8, text: []const u8 };

/// Public policy reads follow the committed authority, while projection and
/// artifact replay retain their independent durable completion watermarks.
/// The returned config borrows its static declaration from cfg and must not be
/// deinitialized separately. Only the requested destination is opened.
pub fn currentConfig(root: *h.Handle, database: *h.db_mod.DB, a: A, cfg: h.db_mod.ResolverConfig, mention: ?MentionPolicy) !h.db_mod.ResolverConfig {
    const destination = tables.get(root, cfg.table) catch |err| switch (err) {
        error.UndefinedTable => null,
        else => return err,
    };
    var snapshot: ?native.Snapshot = null;
    if (destination) |db| {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        var bridge: Bridge = .{ .txn = &txn };
        const store = bridge.store(a, prefix);
        snapshot = if (mention) |point| try store.mentionPolicy(cfg.key_template, point.key, point.text, cfg.aliases) else try store.exportNamespace(cfg.key_template);
        if (snapshot) |*captured| captured.namespace.state.receipt_scope = (try store.queueHead()).receipt_scope;
    }
    if (snapshot == null and cfg.namespace_curation_table_id == 0)
        return (try database.core.index_manager.resolverIdentityAlloc(a, cfg.name)) orelse error.ResolverNotFound;
    const table_id = if (destination) |db| db.core.identity_namespace.table_id else cfg.namespace_curation_table_id;
    if (snapshot == null and destination != null and cfg.namespace_curation_table_id == table_id and !cfg.namespace_curation_missing and cfg.namespace_curation_generation != 0) return error.InvalidMetadataBatch;
    const namespace = if (snapshot) |captured| captured.namespace else policy.Namespace{ .key_template = cfg.key_template };
    var current = cfg;
    current.shared_projection = null;
    current.validation_proof = null;
    current.merges = namespace.merges;
    current.curation_json = try json(a, namespace.state);
    current.curation_revision = namespace.revision;
    current.namespace_curation_generation = namespace.revision;
    current.namespace_curation_table_id = table_id;
    current.namespace_curation_missing = snapshot == null;
    return current;
}

pub const Stamp = struct { revision: u64, fingerprint: [32]u8 };
pub fn namespaceStamp(db: *h.db_mod.DB, alloc: A, template: []const u8) !?Stamp {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const head = (try bridge.store(arena.allocator(), prefix).head(template)) orelse return null;
    return .{ .revision = head.revision, .fingerprint = head.fingerprint };
}
pub fn revision(db: *h.db_mod.DB, alloc: A, template: []const u8) !?u64 {
    return if (try namespaceStamp(db, alloc, template)) |authority_head| authority_head.revision else null;
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
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical != null) {
        try ensureConsumerIndex(root, a);
        var read = try database.core.store.beginReadTxn();
        defer read.abort();
        var bridge: Bridge = .{ .txn = &read };
        return (try bridge.store(a, "").backend.scan(&bridge, a, @import("../storage/lite/resolver_consumers.zig").work_prefix, null, 1)).rows.len != 0;
    }
    for (try database.core.index_manager.listResolverStaticDescriptors(a)) |cfg| if (try projection(root, a, cfg)) |desired| {
        if (desired.changed) return true;
    };
    return false;
}
