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
const ProfileCache = std.StringHashMapUnmanaged([]const native.AliasProfile);
const Consumer = struct { db: *h.db_mod.DB, source: []const u8, cfg: h.db_mod.ResolverConfig, synthetic: bool = false, profile_cache: ?*ProfileCache = null, scope_head: ?native.Head = null };
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
        if (ready and physical.?.read_only) return;
        const floor_ready = blk: {
            var read = try physical.?.beginRead();
            defer read.abort();
            const raw = read.get(catalog.minimum_reader_key) catch |err| if (err == error.NotFound) break :blk false else return err;
            break :blk (std.fmt.parseInt(u32, raw, 10) catch return error.InvalidCatalogRecord) >= catalog.resolver_pin_index_reader_version;
        };
        if (!ready or !floor_ready) {
            var write = try physical.?.beginWrite();
            errdefer write.abort();
            try directory.bootstrap(a, &write);
            try write.put(catalog.minimum_reader_key, try catalog.readerVersionBytes(catalog.resolver_pin_index_reader_version));
            try write.commit();
            root.db.core.index_manager.minimum_reader_version.store(catalog.resolver_pin_index_reader_version, .release);
        }
    }
}
pub var test_admission_binding_visits: std.atomic.Value(usize) = .init(0);
fn consumers(root: *h.Handle, a: A, table: []const u8, key: []const u8, observed_epoch: ?*?u64) ![]const Consumer {
    return consumersForAdmission(root, a, table, key, observed_epoch, null);
}
/// Proposal creation needs membership, not source observations. An adopted
/// namespace is certified by its head and the directory epoch; only cold
/// namespaces require source identities and legacy folding.
fn consumersForAdmission(root: *h.Handle, a: A, table: []const u8, key: []const u8, observed_epoch: ?*?u64, cold_destination: ?*h.db_mod.DB) ![]const Consumer {
    const directory = @import("../storage/lite/resolver_consumers.zig");
    // Native Lite stores share a physical transaction space. Bootstrap legacy
    // bindings once from catalog bytes; warm calls seek only this destination.
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical == null) return legacyConsumers(root, a, table, key);
    try ensureConsumerIndex(root, a);
    var txn = try physical.?.beginRead();
    defer txn.abort();
    if (observed_epoch) |captured_epoch| captured_epoch.* = try directory.epoch(&txn);
    var bindings: std.ArrayList(directory.Binding) = .empty;
    var result: std.ArrayList(Consumer) = .empty;
    var destination_read = if (cold_destination) |db| try db.core.store.beginReadTxn() else null;
    defer if (destination_read) |*read| read.abort();
    for (try directory.matchingNamespaces(a, &txn, table, key)) |template| {
        if (destination_read) |*read| {
            var bridge: Bridge = .{ .txn = read };
            if (try bridge.store(a, prefix).head(template)) |head| {
                try result.append(a, .{ .db = cold_destination.?, .source = "", .synthetic = true, .scope_head = head, .cfg = .{ .name = "native_authority", .table = table, .key_template = template, .source_artifact = "authority_source", .resolution_artifact = "authority_resolution", .promote = false } });
                continue;
            }
        }
        const target = try directory.namespacePrefix(a, table, template);
        var cursor = try txn.openCursor();
        defer cursor.close();
        var row = try directory.seekEntry(&cursor, target);
        while (row) |entry| {
            if (!std.mem.startsWith(u8, entry.key, target)) break;
            if (@import("builtin").is_test) _ = test_admission_binding_visits.fetchAdd(1, .monotonic);
            try bindings.append(a, try std.json.parseFromSliceLeaky(directory.Binding, a, entry.value, .{ .allocate = .alloc_always }));
            row = try directory.nextEntry(&cursor);
        }
    }
    if (bindings.items.len > 0) try tables.loadCatalog(root);
    for (bindings.items) |binding| {
        if (binding.source_id != 0 and root.embedded_table_records.get(binding.source) != binding.source_id) continue;
        const source = try tables.get(root, binding.source);
        const cfg = try source.core.index_manager.resolverAdmissionIdentity(a, binding.resolver) orelse continue;
        if (cfg.config_generation != binding.config_generation or !std.mem.eql(u8, cfg.admission_profile, binding.profile_id) or binding.profile_id.len == 0) return error.ResolverCurationConflict;
        if (!std.mem.eql(u8, cfg.table, table) or !std.mem.eql(u8, cfg.key_template, binding.key_template)) return error.ResolverCurationConflict;
        try result.append(a, .{ .db = source, .source = binding.source, .cfg = cfg });
    }
    return result.items;
}
fn legacyConsumers(root: *h.Handle, a: A, table: []const u8, key: []const u8) ![]const Consumer {
    try tables.load(root);
    var result: std.ArrayList(Consumer) = .empty;
    var sources = root.embedded_tables.iterator();
    const defaults = try root.db.core.index_manager.listResolverAdmissionDescriptors(a);
    for (defaults) |cfg| if (std.mem.eql(u8, cfg.table, table) and policy.keyInNamespace(cfg.key_template, key)) try result.append(a, .{ .db = &root.db, .source = "default", .cfg = cfg });
    while (sources.next()) |source| for (try source.value_ptr.*.db.core.index_manager.listResolverAdmissionDescriptors(a)) |cfg| {
        if (std.mem.eql(u8, cfg.table, table) and policy.keyInNamespace(cfg.key_template, key)) try result.append(a, .{ .db = &source.value_ptr.*.db, .source = source.key_ptr.*, .cfg = cfg });
    };
    return result.items;
}
/// Resolve static adjacency outside writer ownership against a pinned physical
/// index and curated read cut. Publication rechecks both source and head fences.
fn prepareStaticProfiles(root: *h.Handle, a: A, destination: *h.db_mod.DB, table: []const u8, key: []const u8, into: ?[]const u8, inputs: []const Consumer, expected_epoch: ?u64, expand: bool) ![]const Consumer {
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical == null or inputs.len == 0) return prepareProfileCaches(a, inputs);
    const selected = try a.dupe(Consumer, inputs);
    var physical_read = try physical.?.beginRead();
    defer physical_read.abort();
    if (expected_epoch) |expected| if (try @import("../storage/lite/resolver_consumers.zig").epoch(&physical_read) != expected) return error.ResolverCurationConflict;
    var directory: native.static_profiles.ReadTransaction(@TypeOf(physical_read)) = .{ .txn = &physical_read };
    var read = try destination.core.store.beginReadTxn();
    defer read.abort();
    var bridge: Bridge = .{ .txn = &read };
    const store = bridge.store(a, prefix);
    var groups: std.StringHashMapUnmanaged(std.ArrayList(usize)) = .empty;
    for (selected, 0..) |source, i| {
        const group = try groups.getOrPut(a, source.cfg.key_template);
        if (!group.found_existing) group.value_ptr.* = .empty;
        try group.value_ptr.append(a, i);
    }
    const cache = try a.create(ProfileCache);
    cache.* = .empty;
    var definitions: std.StringHashMapUnmanaged(native.static_profiles.Definition) = .empty;
    var iterator = groups.iterator();
    while (iterator.next()) |group| {
        const template = group.key_ptr.*;
        const head = try store.head(template);
        if (head == null) {
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            var profiles: std.ArrayList(native.AliasProfile) = .empty;
            for (group.value_ptr.items) |i| {
                const entry = &selected[i];
                const saved = try definitions.getOrPut(a, entry.cfg.admission_profile);
                if (!saved.found_existing) saved.value_ptr.* = try native.static_profiles.readDefinition(a, &physical_read, try @import("../storage/lite/resolver_consumers.zig").profileRoot(a, table, template), entry.cfg.admission_profile);
                const definition = saved.value_ptr.*;
                entry.cfg.aliases = definition.aliases;
                entry.cfg.suppress_json = definition.suppress_json;
                if ((try seen.getOrPut(a, entry.cfg.admission_profile)).found_existing) continue;
                try profiles.append(a, .{ .id = entry.cfg.admission_profile, .key_template = template, .aliases = definition.aliases, .suppress_json = definition.suppress_json });
            }
            try cache.put(a, template, profiles.items);
            continue;
        }
        if (!expand) {
            for (group.value_ptr.items) |i| selected[i].scope_head = head;
            try cache.put(a, template, &.{});
            continue;
        }
        var indexed: native.static_profiles.Reader = .{ .a = a, .backend = directory.backend(), .key_template = template, .root = try @import("../storage/lite/resolver_consumers.zig").profileRoot(a, table, template) };
        _ = (try store.componentIndexed(template, if (into) |target| &.{ key, target } else &.{key}, indexed.lookup())) orelse return error.ResolverCurationConflict;
        const values = try indexed.values();
        try cache.put(a, template, values);
        // Membership is certified by the enrolled binding/profile identity and
        // the same physical directory epoch. Sparse omission means no incident
        // aliases or negative keys, rather than a missing source declaration.
        var by_id: std.StringHashMapUnmanaged(native.AliasProfile) = .empty;
        for (values) |profile| try by_id.put(a, profile.id, profile);
        for (group.value_ptr.items) |i| {
            const entry = &selected[i];
            entry.scope_head = head;
            entry.cfg.aliases = if (by_id.get(entry.cfg.admission_profile)) |profile| profile.aliases else &.{};
            entry.cfg.suppress_json = if (by_id.get(entry.cfg.admission_profile)) |profile| profile.suppress_json else "";
        }
    }
    for (selected) |*entry| entry.profile_cache = cache;
    return selected;
}

fn checkDirectoryEpoch(root: *h.Handle, expected: ?u64) !void {
    const expected_epoch = expected orelse return;
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical == null) return error.ResolverCurationConflict;
    var read = try physical.?.beginRead();
    defer read.abort();
    if (try @import("../storage/lite/resolver_consumers.zig").epoch(&read) != expected_epoch) return error.ResolverCurationConflict;
}
fn checkScopeHeads(a: A, store: native.Store, selected: []const Consumer) !void {
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (selected) |source| if (source.scope_head) |expected| {
        if ((try seen.getOrPut(a, expected.key_template)).found_existing) continue;
        const current = (try store.head(expected.key_template)) orelse return error.ResolverCurationConflict;
        if (current.revision != expected.revision or !std.mem.eql(u8, &current.fingerprint, &expected.fingerprint)) return error.ResolverCurationConflict;
    };
}
fn ownedConsumers(a: A, store: native.Store, destination: *h.db_mod.DB, current: []const Consumer, table: []const u8, key: []const u8) ![]const Consumer {
    var out: std.ArrayList(Consumer) = .empty;
    try out.appendSlice(a, current);
    const cache = if (current.len > 0 and current[0].profile_cache != null) current[0].profile_cache.? else blk: {
        const empty = try a.create(ProfileCache);
        empty.* = .empty;
        break :blk empty;
    };
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (current) |source| try seen.put(a, source.cfg.key_template, {});
    for (try store.matchingHeads(key)) |head| {
        if (seen.contains(head.key_template) or !policy.keyInNamespace(head.key_template, key)) continue;
        try cache.put(a, head.key_template, try a.dupe(native.AliasProfile, &.{.{ .key_template = head.key_template, .aliases = &.{} }}));
        try out.append(a, .{ .db = destination, .source = "", .synthetic = true, .profile_cache = cache, .cfg = .{ .name = "native_authority", .table = table, .key_template = head.key_template, .source_artifact = "authority_source", .resolution_artifact = "authority_resolution", .promote = false } });
    }
    return out.items;
}
fn initialize(a: A, bridge: *Bridge, store: native.Store, legacy: c.State) !void {
    try bridge.backendPut(a, catalog.minimum_reader_key, try catalog.readerVersionBytes(catalog.resolver_pin_index_reader_version));
    try store.ensureHeadRoutes();
    if (try bridge.backendGet(a, marker) != null) return;
    if (legacy.proposals.len != 0 or legacy.receipt_scope != null) try store.applyQueue(.{ .expected_revision = 0, .revision = 1, .receipt_scope = legacy.receipt_scope, .proposals = legacy.proposals });
    try bridge.backendPut(a, marker, "1");
}
const PreparedAdoption = struct {
    namespaces: []const policy.Namespace = &.{},
    sources: []const Source = &.{},
    const Source = struct { db: *h.db_mod.DB, generation: u64 };
    fn check(self: PreparedAdoption) !void {
        for (self.sources) |source| if (source.db.core.index_manager.write_plan_generation.load(.acquire) != source.generation) return error.ResolverCurationConflict;
    }
};
/// Snapshot only selected definitions before taking a writer lease. The owner
/// fence retains source incarnations; generation checks fence config edits.
fn prepareAdoption(a: A, destination: *h.db_mod.DB, selected: []const Consumer) !PreparedAdoption {
    var read = try destination.core.store.beginReadTxn();
    defer read.abort();
    var bridge: Bridge = .{ .txn = &read };
    const store = bridge.store(a, prefix);
    var namespaces: std.StringArrayHashMapUnmanaged(std.ArrayList(h.db_mod.ResolverConfig)) = .empty;
    var sources: std.AutoHashMapUnmanaged(*h.db_mod.DB, u64) = .empty;
    var profiles: std.StringHashMapUnmanaged(void) = .empty;
    for (selected) |source| {
        if (source.synthetic) continue;
        const fence = try sources.getOrPut(a, source.db);
        if (!fence.found_existing) fence.value_ptr.* = source.db.core.index_manager.write_plan_generation.load(.acquire);
        const identity = try source.db.core.index_manager.resolverAdmissionIdentity(a, source.cfg.name) orelse return error.ResolverCurationConflict;
        if (identity.effectiveGeneration() != source.cfg.effectiveGeneration() or identity.retired != source.cfg.retired or !std.mem.eql(u8, identity.table, source.cfg.table) or !std.mem.eql(u8, identity.key_template, source.cfg.key_template)) return error.ResolverCurationConflict;
        if (try store.head(source.cfg.key_template) != null) continue;
        const cfg = try source.db.core.index_manager.resolverLegacyPolicyDescriptor(a, source.cfg.name) orelse return error.ResolverCurationConflict;
        if (cfg.config_generation != source.cfg.config_generation or cfg.retired != source.cfg.retired or !std.mem.eql(u8, cfg.table, source.cfg.table) or !std.mem.eql(u8, cfg.key_template, source.cfg.key_template)) return error.ResolverCurationConflict;
        const profile = try json(a, .{ cfg.key_template, cfg.merges, cfg.curation_json, cfg.curation_revision });
        if ((try profiles.getOrPut(a, profile)).found_existing) continue;
        const entry = try namespaces.getOrPut(a, cfg.key_template);
        if (!entry.found_existing) {
            entry.key_ptr.* = try a.dupe(u8, cfg.key_template);
            entry.value_ptr.* = .empty;
        }
        try entry.value_ptr.append(a, cfg);
    }
    const fences = try a.alloc(PreparedAdoption.Source, sources.count());
    var it = sources.iterator();
    for (fences) |*source| {
        const entry = it.next().?;
        source.* = .{ .db = entry.key_ptr.*, .generation = entry.value_ptr.* };
    }
    const policies = try a.alloc(policy.Namespace, namespaces.count());
    for (namespaces.keys(), namespaces.values(), policies) |template, definitions, *namespace| namespace.* = try policy.foldNamespaceBatch(a, template, definitions.items);
    const prepared: PreparedAdoption = .{ .namespaces = policies, .sources = fences };
    try prepared.check();
    return prepared;
}
fn adopt(a: A, store: native.Store, prepared: PreparedAdoption) !void {
    _ = a;
    try prepared.check();
    for (prepared.namespaces) |namespace| if (try store.head(namespace.key_template) == null) try store.adoptNamespace(namespace);
}
fn inspectionScope(a: A, store: native.Store, selected: []const Consumer, source: Consumer, key: []const u8) !native.Snapshot {
    if (try store.component(source.cfg.key_template, &.{key})) |snapshot| return snapshot;
    // Read-only legacy inspection folds only this namespace. Its first
    // authorized mutation publishes the native records.
    var builder: policy.NamespaceBuilder = .{ .template = source.cfg.key_template };
    for (selected) |consumer| {
        if (consumer.synthetic or !std.mem.eql(u8, consumer.cfg.key_template, source.cfg.key_template)) continue;
        const cfg = try consumer.db.core.index_manager.resolverLegacyPolicyDescriptor(a, consumer.cfg.name) orelse return error.ResolverCurationConflict;
        try builder.add(a, a, cfg);
    }
    const namespace = try builder.finish(a);
    return .{ .head = .{ .key_template = namespace.key_template, .revision = namespace.revision }, .namespace = namespace };
}
fn consumerProfiles(a: A, selected: []const Consumer, template: []const u8) ![]const native.AliasProfile {
    if (selected.len > 0) if (selected[0].profile_cache) |cache| return cache.get(template) orelse error.InvalidCatalogRecord;
    var profiles: std.ArrayList(native.AliasProfile) = .empty;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (selected) |source| if (std.mem.eql(u8, source.cfg.key_template, template)) {
        const key = try native.admissionProfileKey(a, source.cfg);
        if ((try seen.getOrPut(a, key)).found_existing) continue;
        try profiles.append(a, .{ .id = key, .key_template = template, .aliases = source.cfg.aliases, .suppress_json = source.cfg.suppress_json });
    };
    return profiles.items;
}
fn prepareProfileCaches(a: A, inputs: []const Consumer) ![]const Consumer {
    const Builder = struct { values: std.ArrayList(native.AliasProfile) = .empty, seen: std.StringHashMapUnmanaged(void) = .empty };
    var profiles: std.StringHashMapUnmanaged(Builder) = .empty;
    for (inputs) |source| {
        const slot = try profiles.getOrPut(a, source.cfg.key_template);
        if (!slot.found_existing) slot.value_ptr.* = .{};
        const key = try native.admissionProfileKey(a, source.cfg);
        if ((try slot.value_ptr.seen.getOrPut(a, key)).found_existing) continue;
        try slot.value_ptr.values.append(a, .{ .id = key, .key_template = source.cfg.key_template, .aliases = source.cfg.aliases, .suppress_json = source.cfg.suppress_json });
    }
    const cache = try a.create(ProfileCache);
    cache.* = .empty;
    var it = profiles.iterator();
    while (it.next()) |entry| try cache.put(a, entry.key_ptr.*, entry.value_ptr.values.items);
    const selected = try a.dupe(Consumer, inputs);
    for (selected) |*source| source.profile_cache = cache;
    return selected;
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

fn collectQueueKeys(a: A, keys: *std.StringArrayHashMapUnmanaged(void), namespace: policy.Namespace, absorbed: []const c.AbsorbedResult) !void {
    for (namespace.merges) |alias| {
        try keys.put(a, alias.key, {});
        try keys.put(a, alias.target, {});
    }
    for (namespace.state.suppress.keys) |id| try keys.put(a, id, {});
    for (absorbed) |result| try keys.put(a, result.key, {});
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
fn publishQueue(a: A, store: native.Store, before: c.State, next: c.State, retain_evidence: bool) !void {
    var changed: std.ArrayList(c.Proposal) = .empty;
    var old: std.StringHashMapUnmanaged(c.Proposal) = .empty;
    for (before.proposals) |proposal| try old.put(a, proposal.id, proposal);
    for (next.proposals) |proposal| {
        if (old.get(proposal.id)) |previous| if (std.mem.eql(u8, try json(a, previous), try json(a, proposal))) continue;
        var header = proposal;
        if (retain_evidence) header.evidence = null;
        try changed.append(a, header);
    }
    if (changed.items.len == 0) return;
    const head = try store.queueHead();
    try store.applyQueue(.{ .expected_revision = head.revision, .revision = try std.math.add(u64, head.revision, 1), .receipt_scope = next.receipt_scope, .proposals = changed.items, .retain_evidence = retain_evidence });
}
const ResultHeads = std.StringHashMapUnmanaged(native.Head);
fn resultHeads(a: A, selected: []const Consumer, store: native.Store) !ResultHeads {
    var heads: ResultHeads = .empty;
    for (selected) |source| {
        if (source.synthetic) continue;
        const slot = try heads.getOrPut(a, source.cfg.key_template);
        if (!slot.found_existing) slot.value_ptr.* = (try store.head(source.cfg.key_template)) orelse return error.ResolverNotFound;
    }
    return heads;
}
const ReplayObservations = struct {
    values: std.AutoHashMapUnmanaged(*h.db_mod.DB, ?h.db_mod.types.ResolverReplayObservation) = .empty,
    fn applied(self: *@This(), a: A, source: Consumer) !bool {
        if (source.cfg.retired) return true;
        const slot = try self.values.getOrPut(a, source.db);
        if (!slot.found_existing) slot.value_ptr.* = source.db.resolverReplayObservation(a) catch null;
        return if (slot.value_ptr.*) |observation| observation.applied(source.cfg.promote) else false;
    }
};
fn results(a: A, authority_table_id: u64, selected: []const Consumer, store: native.Store, key: []const u8, redirects: bool, changed: bool, heads: ResultHeads) !std.json.Value {
    var rows = std.json.Array.init(a);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var observations: ReplayObservations = .{};
    for (selected) |source| {
        if (redirects) {
            if ((try seen.getOrPut(a, source.cfg.key_template)).found_existing) continue;
            const snapshot = try inspectionScope(a, store, selected, source, key);
            const scoped = try c.entityState(a, snapshot.namespace.state, snapshot.namespace.merges, key);
            try rows.append(try value(a, .{ .source_table = source.cfg.table, .resolver = "", .key_template = source.cfg.key_template, .revision = snapshot.head.revision, .redirects = try c.redirectsWithOrigins(a, scoped.redirects, scoped.curation, snapshot.namespace.merges), .canonical_key = scoped.canonical_key, .curation = scoped.curation }));
        } else {
            if (source.synthetic) continue;
            const head = heads.get(source.cfg.key_template) orelse return error.ResolverNotFound;
            try rows.append(try value(a, .{ .source_table = source.source, .resolver = source.cfg.name, .key_template = source.cfg.key_template, .revision = head.revision, .accepted = true, .changed = changed, .applied = !source.cfg.namespace_curation_missing and source.cfg.namespace_curation_table_id == authority_table_id and source.cfg.namespace_curation_generation >= head.revision and try observations.applied(a, source), .replay_sequence = source.db.core.store.lastReplaySequence(0), .replay_sequence_kind = "source_journal" }));
        }
    }
    return .{ .array = rows };
}
fn namespaceOutcomes(a: A, store: native.Store, absorbed: AbsorbedMap, changed: bool) !std.json.Value {
    var values = std.json.Array.init(a);
    var entries = absorbed.iterator();
    while (entries.next()) |entry| {
        const head = (try store.head(entry.key_ptr.*)) orelse return error.ResolverNotFound;
        try values.append(try value(a, .{ .key_template = entry.key_ptr.*, .revision = head.revision, .changed = changed, .absorbed = entry.value_ptr.* }));
    }
    return .{ .array = values };
}
fn commitCurationTxn(a: A, bridge: *Bridge, txn: *docstore.DocStore.Txn, lease: anytype) !void {
    try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
    try bridge.flush(a);
    try txn.commit();
}
fn finish(a: A, bridge: *Bridge, txn: *docstore.DocStore.Txn, db: *h.db_mod.DB, lease: anytype) !void {
    if (!try bridge.store(a, prefix).ownershipIndexed()) _ = try bridge.store(a, prefix).advanceOwnershipIndex(128);
    try commitCurationTxn(a, bridge, txn, lease);
    db.core.index_manager.minimum_reader_version.store(catalog.resolver_pin_index_reader_version, .release);
}
pub fn entity(root: *h.Handle, a: A, table: []const u8, key: []const u8, input: Input, legacy: c.State) ![]const u8 {
    const db = try tables.get(root, table);
    const reading = input.action == .redirects;
    if (!reading and try db.maintenanceRequiresOrderedApply()) return error.OrderedResolverReviewRequired;
    var selected: []const Consumer = &.{};
    var directory_epoch: ?u64 = null;
    if (!reading) {
        selected = try consumers(root, a, table, key, &directory_epoch);
        selected = try prepareStaticProfiles(root, a, db, table, key, input.into, selected, directory_epoch, true);
    }
    const prepared = if (reading) PreparedAdoption{} else try prepareAdoption(a, db, selected);
    var lease = if (reading) try db.local_execution.row_policy_gate.enterRawRead() else try db.local_execution.row_policy_gate.enterRaw();
    defer lease.release();
    var txn = if (reading) try db.core.store.beginReadTxn() else try db.core.store.beginWriteTxn();
    var txn_live = true;
    defer if (txn_live) txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const store = bridge.store(a, prefix);
    if (!reading) {
        try checkDirectoryEpoch(root, directory_epoch);
        try checkScopeHeads(a, store, selected);
        try initialize(a, &bridge, store, legacy);
        try adopt(a, store, prepared);
    }
    selected = try ownedConsumers(a, store, db, selected, table, key);
    if (reading and selected.len == 0) {
        txn.abort();
        txn_live = false;
        // Consumer discovery may bootstrap its physical reverse index and
        // open matching sources. Do that before pinning the destination read.
        selected = try consumers(root, a, table, key, null);
        txn = try db.core.store.beginReadTxn();
        txn_live = true;
    }
    if (selected.len == 0) return error.ResolverNotFound;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var changed = false;
    var absorbed: AbsorbedMap = .empty;
    const Pending = struct { before: native.Snapshot, plan: c.Plan, request: c.Request, origin: c.Origin };
    var pending: std.ArrayList(Pending) = .empty;
    var queue_keys: std.StringArrayHashMapUnmanaged(void) = .empty;
    try queue_keys.put(a, key, {});
    if (input.into) |target| try queue_keys.put(a, target, {});
    const queue_head = try store.queueHead();
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
        const state = c.bindReceiptScope(before.namespace.state, queue_head.receipt_scope);
        const plan = try c.planWithOrigin(a, try json(a, state), before.namespace.merges, try native.planningAliases(a, before.namespace, try aliases(a, selected, before.namespace, key, input.into), action, key, input.into), request, stamp);
        try collectQueueKeys(a, &queue_keys, before.namespace, plan.absorbed);
        try pending.append(a, .{ .before = before, .plan = plan, .request = request, .origin = stamp });
        try absorbed.put(a, source.cfg.key_template, plan.absorbed);
        changed = changed or plan.changed;
    }
    if (!reading) {
        const queue = try store.proposalHeadersForKeys(queue_keys.keys());
        var batch = try c.SupersessionBatch.init(a, queue.proposals);
        for (pending.items) |item| {
            if (item.plan.changed) try batch.apply(item.request, item.plan.absorbed, item.origin);
            _ = try publishPlan(a, store, selected, item.before, item.plan);
        }
        var next_queue = queue;
        next_queue.proposals = batch.values();
        try publishQueue(a, store, queue, next_queue, true);
    }
    const heads = if (reading) ResultHeads.empty else try resultHeads(a, selected, store);
    const outcomes = try namespaceOutcomes(a, store, absorbed, changed);
    if (!reading) {
        try prepared.check();
        try finish(a, &bridge, &txn, db, &lease);
        txn_live = false;
    } else try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
    const rows = try results(a, db.core.identity_namespace.table_id, selected, store, key, reading, changed, heads);
    return json(a, .{ .entity_table = table, .key = key, .atomicity = "namespace", .complete = true, .namespaces = outcomes, .results = rows });
}
/// Read/write queues use the same bounded indexes as routed admission.
pub fn proposals(root: *h.Handle, a: A, operation: Operation, table: []const u8, id: []const u8, raw: []const u8, legacy: c.State) ![]const u8 {
    const db = try tables.get(root, table);
    const reading = operation == .list or operation == .get;
    if (!reading and try db.maintenanceRequiresOrderedApply()) return error.OrderedResolverReviewRequired;
    // Open matching sources before the destination write transaction. All
    // namespaces share the native file writer; queue-only decisions need none.
    var selected: []const Consumer = &.{};
    var directory_epoch: ?u64 = null;
    var needs_policy = false;
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
        needs_policy = if (current) |proposal| proposal.state == .open else true;
        const terminal_policy = if (current) |proposal| proposal.state != .open and !(queue_only_receipt or (proposal.state == .rejected and proposal.action != .merge)) else false;
        const need_sources = if (create != null) current == null else decision.?.accept or decision.?.pin_distinct or decision.?.expected_revision != null or terminal_policy;
        if (need_sources) {
            const key = if (current) |proposal| proposal.key else if (create) |request| request.key else return error.NotFound;
            selected = try consumersForAdmission(root, a, table, key, &directory_epoch, if (create != null) db else null);
            const into = if (current) |proposal| proposal.into else if (create) |request| request.into else null;
            if (needs_policy) selected = try prepareStaticProfiles(root, a, db, table, key, into, selected, directory_epoch, operation == .decide);
        }
    }
    const prepared = if (needs_policy) try prepareAdoption(a, db, selected) else PreparedAdoption{};
    var lease = if (reading) try db.local_execution.row_policy_gate.enterRawRead() else try db.local_execution.row_policy_gate.enterRaw();
    defer lease.release();
    var txn = if (reading) try db.core.store.beginReadTxn() else try db.core.store.beginWriteTxn();
    var txn_live = true;
    defer if (txn_live) txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const store = bridge.store(a, prefix);
    if (!reading) {
        try checkDirectoryEpoch(root, directory_epoch);
        try checkScopeHeads(a, store, selected);
        try initialize(a, &bridge, store, legacy);
    }
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
                try publishQueue(a, store, .{ .proposals = &.{proposal} }, .{ .receipt_scope = (try store.queueHead()).receipt_scope, .proposals = &.{decided} }, false);
                try store.markQueueOnly(proposal.id);
                try prepared.check();
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
        const heads = try resultHeads(a, consumers_with_heads.items, store);
        const queue_only = try store.queueOnly(proposal.id) or (!decision.accept and proposal.action != .merge);
        txn.abort();
        txn_live = false;
        const rows = try results(a, db.core.identity_namespace.table_id, consumers_with_heads.items, store, key, false, false, heads);
        const applied = if (queue_only) true else for (rows.array.items) |row| {
            if (!row.object.get("applied").?.bool) break false;
        } else true;
        try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
        return json(a, .{ .proposal = proposal, .atomicity = "namespace", .changed = false, .complete = true, .applied = applied, .results = rows });
    }
    try adopt(a, store, prepared);
    var head = try store.queueHead();
    if (creation) |request| {
        if (head.receipt_scope == null) {
            var entropy: [16]u8 = undefined;
            try @import("antfly_platform").entropy.fill(h.handleLockIo(), &entropy);
            head.receipt_scope = std.fmt.bytesToHex(entropy, .lower);
        }
        const plan = try c.propose(a, .{ .receipt_scope = head.receipt_scope }, &.{}, request, policy.manualOrigin(.{ .action = .suppress, .actor = request.actor, .reason = request.reason }, 0));
        try publishQueue(a, store, .{}, plan.state, false);
        existing = plan.state.proposals[0];
        try prepared.check();
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
    var decision_origin = policy.manualOrigin(.{ .action = .suppress, .actor = decision.actor, .reason = decision.reason }, 0);
    decision_origin.origin = .review;
    decision_origin.proposal_id = proposal.id;
    var changed = false;
    var absorbed: AbsorbedMap = .empty;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    const Pending = struct { before: native.Snapshot, plan: c.Plan, origin: c.Origin };
    var pending: std.ArrayList(Pending) = .empty;
    var queue_keys: std.StringArrayHashMapUnmanaged(void) = .empty;
    try queue_keys.put(a, key, {});
    if (proposal.into) |target| try queue_keys.put(a, target, {});
    const intent = c.decisionRequest(proposal, decision);
    if (proposal.state == .open) for (selected) |source| {
        if ((try seen.getOrPut(a, source.cfg.key_template)).found_existing) continue;
        const before = try profileScope(a, store, selected, source.cfg.key_template, key, proposal.into);
        if (decision.expected_revision) |expected| if (expected != before.head.revision) return error.ResolverCurationConflict;
        var state = c.bindReceiptScope(before.namespace.state, head.receipt_scope);
        var reviewed_header = proposal;
        reviewed_header.evidence = null;
        state.proposals = &.{reviewed_header};
        const origin = policy.manualOrigin(.{ .action = .suppress, .actor = decision.actor, .reason = decision.reason }, try std.math.add(u64, before.head.revision, 1));
        const plan = try c.decide(a, state, before.namespace.merges, try native.planningAliases(a, before.namespace, try aliases(a, selected, before.namespace, key, proposal.into), intent.action, key, proposal.into), proposal.id, decision, origin);
        var review_origin = origin;
        review_origin.origin = .review;
        review_origin.proposal_id = proposal.id;
        try collectQueueKeys(a, &queue_keys, before.namespace, plan.absorbed);
        try pending.append(a, .{ .before = before, .plan = plan, .origin = review_origin });
        try absorbed.put(a, source.cfg.key_template, plan.absorbed);
        changed = changed or plan.changed;
    };
    if (pending.items.len != 0) {
        const queue = try store.proposalHeadersForKeys(queue_keys.keys());
        var batch = try c.SupersessionBatch.init(a, queue.proposals);
        for (pending.items) |item| {
            try batch.apply(intent, item.plan.absorbed, item.origin);
            _ = try publishPlan(a, store, selected, item.before, item.plan);
        }
        var explicit = proposal;
        explicit.state = wanted;
        explicit.decided = decision_origin;
        try batch.decide(explicit);
        var next_queue = queue;
        next_queue.proposals = batch.values();
        try publishQueue(a, store, queue, next_queue, true);
    }
    var decided = (try store.proposal(proposal.id)) orelse proposal;
    if (decided.state == .open) {
        decided.state = wanted;
        decided.decided = decision_origin;
        try publishQueue(a, store, .{ .proposals = &.{proposal} }, .{ .receipt_scope = head.receipt_scope, .proposals = &.{decided} }, false);
    }
    const heads = try resultHeads(a, selected, store);
    const outcomes = try namespaceOutcomes(a, store, absorbed, changed);
    try prepared.check();
    try finish(a, &bridge, &txn, db, &lease);
    txn_live = false;
    const rows = try results(a, db.core.identity_namespace.table_id, selected, store, key, false, changed, heads);
    const applied = if (!decision.accept and !decision.pin_distinct) true else for (rows.array.items) |row| {
        if (!row.object.get("applied").?.bool) break false;
    } else true;
    return json(a, .{ .proposal = decided, .atomicity = "namespace", .changed = changed, .complete = true, .applied = applied, .namespaces = outcomes, .results = if (!decision.accept and !decision.pin_distinct) std.json.Value{ .array = std.json.Array.init(a) } else rows });
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
const query_pins_owner_marker = prefix ++ "query-pins-owner-enrolled-v1";
fn queryPinsWorkPossible(database: *h.db_mod.DB, a: A) !bool {
    if (!database.core.index_manager.hasResolverDefinitions()) return false;
    _ = database.core.store.get(a, query_pins_owner_marker) catch |err| return if (err == error.NotFound) true else err;
    return false;
}
fn enrollQueryPinWork(database: *h.db_mod.DB, a: A) !void {
    const jobs = @import("../storage/lite/resolver_consumers.zig");
    const configs = try database.core.index_manager.listResolverIdentities(a);
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var lease = try database.local_execution.row_policy_gate.enterRaw();
    defer lease.release();
    var write = try database.core.store.beginWriteTxn();
    errdefer write.abort();
    for (configs) |cfg| {
        if (cfg.retired) continue;
        const identity = try json(a, .{ cfg.table, cfg.key_template });
        if ((try seen.getOrPut(a, identity)).found_existing) continue;
        try jobs.enqueue(a, &write, "", .{ .table = cfg.table, .key_template = cfg.key_template });
    }
    var bridge: Bridge = .{ .txn = &write };
    try bridge.backendPut(a, query_pins_owner_marker, "1");
    try commitCurationTxn(a, &bridge, &write, &lease);
}
fn yieldQueryPinWork(database: *h.db_mod.DB, a: A, key: []const u8) !void {
    var lease = try database.local_execution.row_policy_gate.enterRaw();
    defer lease.release();
    var write = try database.core.store.beginWriteTxn();
    errdefer write.abort();
    var bridge: Bridge = .{ .txn = &write };
    try bridge.backendPut(a, @import("../storage/lite/resolver_consumers.zig").cursor_key, key);
    try commitCurationTxn(a, &bridge, &write, &lease);
}
/// The native destination owns the derived rows; the immutable profile snapshot
/// comes from the shared directory. Each publication holds the destination's
/// ordinary writer grant and leaves the source job outstanding until complete.
fn advanceSourceQueryPins(root: *h.Handle, database: *h.db_mod.DB, a: A, target: []const u8, template: []const u8) !bool {
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    const file = physical orelse return true;
    for (try database.core.index_manager.resolverNamespaceAdmissionIdentities(a, target, template)) |cfg| {
        const destination = tables.get(root, cfg.table) catch |err| if (err == error.UndefinedTable) continue else return err;
        var read = try file.beginRead();
        defer read.abort();
        const profile_root = try @import("../storage/lite/resolver_consumers.zig").profileRoot(a, cfg.table, cfg.key_template);
        const id = cfg.admission_profile;
        if (id.len != 64) return error.InvalidCatalogRecord;
        if (!try native.static_profiles.queryReferenceActive(a, &read, profile_root, id, cfg.key_template) or !try native.static_profiles.queryNodesReady(a, &read, profile_root, id)) continue;
        var directory: native.static_profiles.ReadTransaction(@TypeOf(read)) = .{ .txn = &read };
        var reader: native.static_profiles.QueryReader = .{ .backend = directory.backend(), .root = profile_root, .profile = id, .indexed = true };
        {
            var probe = try destination.core.store.beginReadTxn();
            defer probe.abort();
            var bridge: Bridge = .{ .txn = &probe };
            if (try bridge.store(a, prefix).queryPinsIndexed(cfg.key_template, id) and try bridge.store(a, prefix).queryPinsCleanupCurrent(cfg.key_template, reader.lookup())) continue;
        }
        var lease = try destination.local_execution.row_policy_gate.enterRaw();
        defer lease.release();
        var write = try destination.core.store.beginWriteTxn();
        errdefer write.abort();
        // The directory read preceded writer admission. Revalidate its active
        // profile set at the current physical cut before publishing any GC or
        // members; a concurrent source edit must not retire a newly live index.
        const summary_key = try std.mem.concat(a, u8, &.{ profile_root, "summary" });
        const captured_summary = read.get(summary_key) catch |err| return if (err == error.NotFound) error.EnrichmentSourceChanged else err;
        var current = try file.beginRead();
        defer current.abort();
        const current_summary = current.get(summary_key) catch |err| return if (err == error.NotFound) error.EnrichmentSourceChanged else err;
        if (!std.mem.eql(u8, captured_summary, current_summary)) return error.EnrichmentSourceChanged;
        var bridge: Bridge = .{ .txn = &write };
        const complete = try bridge.store(a, prefix).advanceQueryPins(cfg.key_template, id, reader.lookup(), 128);
        try bridge.backendPut(a, catalog.minimum_reader_key, try catalog.readerVersionBytes(catalog.resolver_pin_index_reader_version));
        try commitCurationTxn(a, &bridge, &write, &lease);
        destination.core.index_manager.minimum_reader_version.store(catalog.resolver_pin_index_reader_version, .release);
        if (!complete) return false;
    }
    return true;
}

pub fn pullProjection(root: *h.Handle, database: *h.db_mod.DB, a: A) !bool {
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical == null) return legacyPullProjection(root, database, a);
    if (try queryPinsWorkPossible(database, a)) {
        try ensureConsumerIndex(root, a);
        try enrollQueryPinWork(database, a);
        // Enrolling an idle upgrade must not consume the namespace's fair
        // replay turn or let a continuously dirty neighbor block other work.
    }
    if (try ownershipWorkPossible(database, a)) {
        var lease = try database.local_execution.row_policy_gate.enterRaw();
        defer lease.release();
        var write = try database.core.store.beginWriteTxn();
        errdefer write.abort();
        var update: Bridge = .{ .txn = &write };
        _ = try update.store(a, prefix).advanceOwnershipIndex(128);
        // This optional derived cache does not raise an older file's reader
        // floor or bootstrap a cross-source directory merely through polling.
        try commitCurationTxn(a, &update, &write, &lease);
        return true;
    }
    if (!try projectionWorkPossible(database, a)) return false;
    // Orphaned cleanup jobs need no consumer directory. Bootstrapping it
    // would publish a newer reader floor solely to delete obsolete work.
    if (database.core.index_manager.hasResolverDefinitions()) try ensureConsumerIndex(root, a);
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
    if (!desired.missing and !try advanceSourceQueryPins(root, database, a, cfg.table, cfg.key_template)) {
        try yieldQueryPinWork(database, a, row.key);
        return true;
    }
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
    const configs = try database.core.index_manager.listResolverAdmissionDescriptors(a);
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

/// Query declarations remain light; current policy and final validation use
/// destination-owned native heads, independently of projection replay.
pub const QuerySession = struct {
    const resolver = @import("antfly_resolver");
    const runtime = @import("../storage/db/resolution_runtime.zig");
    const Target = struct { table: []const u8, template: []const u8, table_id: u64, stamp: ?Stamp };
    a: A,
    root: *h.Handle,
    database: *h.db_mod.DB,
    selected: h.db_mod.ResolverConfig,
    definitions: [32]u8,
    source_schema: SchemaStamp,
    targets: std.StringHashMapUnmanaged(Target) = .empty,
    schemas: std.StringHashMapUnmanaged(SchemaStamp) = .empty,
    legacy: std.StringHashMapUnmanaged([32]u8) = .empty,
    profiles: std.StringHashMapUnmanaged([]const u8) = .empty,
    fn identity(a: A, table: []const u8, template: []const u8) ![]const u8 {
        return std.fmt.allocPrint(a, "{s}\x00{s}", .{ table, template });
    }
    fn runs(selected: h.db_mod.ResolverConfig, cfg: h.db_mod.ResolverConfig) bool {
        return std.mem.eql(u8, cfg.name, selected.name) or (selected.sharesRoutingDomain(cfg) and std.mem.eql(u8, selected.source_artifact, cfg.source_artifact) and cfg.source_artifact_kind.matches(.asset));
    }
    fn declarations(a: A, configs: []const h.db_mod.ResolverConfig, selected: h.db_mod.ResolverConfig) ![32]u8 {
        var hash = @import("antfly_hash").Sha256.init(.{});
        for (configs) |cfg| if (std.mem.eql(u8, cfg.name, selected.name) or (selected.sharesRoutingDomain(cfg) and std.mem.eql(u8, selected.source_artifact, cfg.source_artifact) and cfg.source_artifact_kind.overlaps(selected.source_artifact_kind))) hash.update(&try cfg.queryDefinitionFingerprint(a));
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    /// Epochs are scoped to the physical DB instance, never persisted or used
    /// across a new snapshot. A changed epoch requalifies the content digest;
    /// replay alone therefore cannot invalidate unchanged schema definitions.
    const SchemaStamp = struct {
        instance: u64,
        core: usize,
        namespace: h.db_mod.DocIdentityNamespace,
        catalog: u64,
        schema_generation: u64,
        schema_version: ?u32,
        digest: [32]u8,
        fn current(database: *h.db_mod.DB) SchemaStamp {
            const generation = database.core.schemaNamespaceGeneration();
            var view = database.core.acquireSchemaView();
            defer if (view) |*pinned| pinned.release();
            return .{ .instance = database.querySchemaIdentity(), .core = @intFromPtr(database.core), .namespace = database.core.identity_namespace, .catalog = database.core.index_manager.writePlanGeneration(), .schema_generation = generation, .schema_version = if (view) |pinned| pinned.tableSchema().version else null, .digest = undefined };
        }
        fn epoch(self: SchemaStamp) h.db_mod.DB.ResolverQuerySchemaEpoch {
            return .{ .instance = self.instance, .core = self.core, .namespace = self.namespace, .catalog = self.catalog, .schema_generation = self.schema_generation, .schema_version = self.schema_version };
        }
        fn matchesEpoch(self: SchemaStamp, database: *h.db_mod.DB) bool {
            const now = current(database);
            return self.instance == now.instance and self.core == now.core and std.meta.eql(self.namespace, now.namespace) and self.catalog == now.catalog and self.schema_generation == now.schema_generation and self.schema_version == now.schema_version;
        }
        fn verify(self: *SchemaStamp, database: *h.db_mod.DB, owner: A) !void {
            if (self.matchesEpoch(database)) return;
            const now = try QuerySession.schema(database, owner);
            if (!std.mem.eql(u8, &self.digest, &now.digest)) return error.Busy;
            self.* = now;
        }
    };
    fn schema(database: *h.db_mod.DB, owner: A) !SchemaStamp {
        @import("antfly_platform").sync.lockYielding(&database.resolver_query_schema_mutex);
        defer database.resolver_query_schema_mutex.unlock();
        var result = SchemaStamp.current(database);
        if (database.resolver_query_schema_epoch) |epoch| if (std.meta.eql(epoch, result.epoch())) {
            result.digest = database.resolver_query_schema_digest;
            if (!result.matchesEpoch(database)) return error.Busy;
            return result;
        };
        var scratch = std.heap.ArenaAllocator.init(owner);
        defer scratch.deinit();
        const a = scratch.allocator();
        const indexes = try database.listIndexes(a);
        defer h.db_mod.types.freeIndexConfigs(a, indexes);
        const enrichments = try database.listEnrichments(a);
        defer h.db_mod.types.freeEnrichmentConfigs(a, enrichments);
        result.digest = try resolver.fingerprint(a, .{ database.core.identity_namespace, try database.getSchemaJson(a), indexes, enrichments });
        if (!result.matchesEpoch(database)) return error.Busy;
        database.resolver_query_schema_digest = result.digest;
        database.resolver_query_schema_epoch = result.epoch();
        return result;
    }
    const Admission = @import("../storage/db/catalog/index_manager.zig").IndexManager.QueryAdmission;
    pub fn init(a: A, handle: *h.Handle, selected: h.db_mod.ResolverConfig, configs: []const h.db_mod.ResolverConfig) !QuerySession {
        return initInternal(a, handle, selected, configs, null, true);
    }
    pub fn initAdmitted(a: A, handle: *h.Handle, selected: h.db_mod.ResolverConfig, admission: *const Admission) !QuerySession {
        return initInternal(a, handle, selected, admission.configs, admission, true);
    }
    pub fn initRenderAdmitted(a: A, handle: *h.Handle, selected: h.db_mod.ResolverConfig, admission: *const Admission) !QuerySession {
        return initInternal(a, handle, selected, admission.configs, admission, false);
    }
    fn initInternal(a: A, handle: *h.Handle, selected: h.db_mod.ResolverConfig, configs: []const h.db_mod.ResolverConfig, admission: ?*const Admission, include_siblings: bool) !QuerySession {
        const root = handle.parent_handle orelse handle;
        var result: QuerySession = .{ .a = a, .root = root, .database = handle.database(), .selected = selected, .definitions = if (admission) |cut| cut.fingerprint else try declarations(a, configs, selected), .source_schema = try schema(handle.database(), root.alloc) };
        for (configs, 0..) |cfg, index| {
            if (!runs(selected, cfg) or (!include_siblings and !std.mem.eql(u8, cfg.name, selected.name))) continue;
            if (cfg.aliases.len != 0 or cfg.suppress_json.len != 0) try result.profiles.put(a, cfg.name, if (admission) |cut| cut.profile(index) else try (native.static_profiles.Definition{ .key_template = cfg.key_template, .aliases = cfg.aliases, .suppress_json = cfg.suppress_json }).identity(a));
            const db = try tables.get(root, cfg.table);
            const key = try identity(a, cfg.table, cfg.key_template);
            if (!result.targets.contains(key)) try result.targets.put(a, key, .{ .table = cfg.table, .template = cfg.key_template, .table_id = db.core.identity_namespace.table_id, .stamp = try namespaceStamp(db, a, cfg.key_template) });
            if (result.targets.get(key).?.stamp == null) {
                const legacy = try currentConfig(root, handle.database(), a, cfg, null);
                try result.legacy.put(a, cfg.name, try resolver.fingerprint(a, .{ legacy.merges, legacy.curation_json, legacy.curation_revision }));
            }
            if (!result.schemas.contains(cfg.table)) try result.schemas.put(a, cfg.table, if (db == handle.database()) result.source_schema else try schema(db, root.alloc));
        }
        return result;
    }
    pub fn verify(self: *QuerySession, handle: *h.Handle) !void {
        const root = handle.parent_handle orelse handle;
        var scratch = std.heap.ArenaAllocator.init(root.alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        if (!std.mem.eql(u8, &self.definitions, &try handle.database().core.index_manager.resolverQueryDeclarationsFingerprint(self.selected))) return error.Busy;
        try self.source_schema.verify(handle.database(), root.alloc);
        var schema_entries = self.schemas.iterator();
        while (schema_entries.next()) |entry| {
            const db = try tables.get(root, entry.key_ptr.*);
            if (db == handle.database()) entry.value_ptr.* = self.source_schema else try entry.value_ptr.verify(db, root.alloc);
        }
        var it = self.targets.valueIterator();
        while (it.next()) |target| {
            const db = try tables.get(root, target.table);
            if (db.core.identity_namespace.table_id != target.table_id) return error.Busy;
            const current = try namespaceStamp(db, a, target.template);
            if (!std.meta.eql(current, target.stamp)) return error.Busy;
        }
        var legacy_entries = self.legacy.iterator();
        while (legacy_entries.next()) |entry| {
            const cfg = (try handle.database().core.index_manager.resolverDescriptor(a, entry.key_ptr.*, false)) orelse return error.Busy;
            defer {
                var owned = cfg;
                owned.deinit(a);
            }
            const legacy = try currentConfig(root, handle.database(), a, cfg, null);
            if (!std.mem.eql(u8, entry.value_ptr, &try resolver.fingerprint(a, .{ legacy.merges, legacy.curation_json, legacy.curation_revision }))) return error.Busy;
        }
        self.root = root;
        self.database = handle.database();
    }
    pub fn prepareMention(self: *QuerySession, a: A, cfg: *const h.db_mod.ResolverConfig, target: *resolver.Resolver, mention: resolver.ExtractedEntity) !void {
        try prepare(self, cfg, a, target, &.{mention}, null);
    }
    pub fn namespaceObservation(self: *QuerySession, a: A, cfg: h.db_mod.ResolverConfig) !struct { table_id: u64, revision: u64 } {
        const target = self.targets.get(try identity(a, cfg.table, cfg.key_template)) orelse return error.Busy;
        return .{ .table_id = target.table_id, .revision = if (target.stamp) |stamp| stamp.revision else cfg.namespace_curation_generation };
    }
    pub fn loader(self: *QuerySession) runtime.QueryPolicy {
        return .{ .ptr = self, .prepare = prepare, .replaces_configured_policy = true };
    }
    fn prepare(ptr: *anyopaque, cfg: *const h.db_mod.ResolverConfig, a: A, target: *resolver.Resolver, entities: []const resolver.ExtractedEntity, candidates: ?[]const []const resolver.Candidate) !void {
        const self: *QuerySession = @ptrCast(@alignCast(ptr));
        const captured = self.targets.get(try identity(a, cfg.table, cfg.key_template)) orelse return error.Busy;
        const db = try tables.get(self.root, cfg.table);
        if (db.core.identity_namespace.table_id != captured.table_id) return error.Busy;
        var seeds: std.StringArrayHashMapUnmanaged(void) = .empty;
        var texts: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (entities) |mention| {
            try texts.put(a, mention.text, {});
            const key = target.renderKeyAlloc(a, mention) catch |err| switch (err) {
                error.EmptyEntityKey => continue,
                else => return err,
            };
            try seeds.put(a, key, {});
        }
        if (candidates) |lists| for (lists) |list| for (list) |candidate| {
            try seeds.put(a, candidate.doc_ref.key, {});
            if (candidate.resolved_doc_ref) |ref| try seeds.put(a, ref.key, {});
        };
        var current = cfg.*;
        var complete_aliases = false;
        var complete_suppression = false;
        if (captured.stamp) |expected| {
            const LiteTxn = @import("../storage/lite/docstore.zig").Txn;
            var profile_txn: ?LiteTxn = null;
            defer if (profile_txn) |*txn| txn.abort();
            var directory: native.static_profiles.ReadTransaction(LiteTxn) = undefined;
            var indexed_profile: native.static_profiles.QueryReader = undefined;
            var profile_lookup: ?native.AliasLookup = null;
            if ((cfg.aliases.len != 0 or cfg.suppress_json.len != 0) and (seeds.count() != 0 or texts.count() != 0)) {
                const physical = if (self.root.owned_lite_backend) |*backend| backend.native_docstore else null;
                if (physical) |file| {
                    profile_txn = try file.beginRead();
                    directory = .{ .txn = &profile_txn.? };
                    const profile_id = self.profiles.get(cfg.name) orelse return error.Busy;
                    const profile_root = try @import("../storage/lite/resolver_consumers.zig").profileRoot(a, cfg.table, cfg.key_template);
                    if (try native.static_profiles.queryReferenceActive(a, &profile_txn.?, profile_root, profile_id, cfg.key_template) and try native.static_profiles.queryNodesReady(a, &profile_txn.?, profile_root, profile_id) and try native.static_profiles.querySuppressionReady(a, &profile_txn.?, profile_root, profile_id)) {
                        indexed_profile = .{ .backend = directory.backend(), .root = profile_root, .profile = profile_id, .indexed = true };
                        profile_lookup = indexed_profile.lookup();
                    }
                }
            }
            // The immutable profile snapshot lives for this entire phase.
            // Native heads are independently rechecked at every query page.
            var prepared: ?native.Store.PreparedQuery = null;
            defer if (prepared) |query| query.release();
            if (cfg.aliases.len != 0 and seeds.count() != 0) {
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                var bridge: Bridge = .{ .txn = &txn };
                var inline_profile: native.Store.InlineQueryProfile = .{ .aliases = cfg.aliases };
                const profile_id = self.profiles.get(cfg.name) orelse return error.Busy;
                const profile: native.Store.QueryProfile = if (profile_lookup) |lookup|
                    .{ .id = profile_id, .lookup = lookup }
                else
                    .{ .id = profile_id, .ptr = &inline_profile, .load = native.Store.InlineQueryProfile.load, .estimated_bytes = inline_profile.estimate() };
                prepared = (try bridge.store(a, prefix).prepareQuery(cfg.key_template, profile, candidates != null)) orelse return error.Busy;
                if (prepared.?.stamp.revision != expected.revision or !std.mem.eql(u8, &prepared.?.stamp.fingerprint, &expected.fingerprint)) return error.Busy;
            }
            var joined: native.QueryPolicyUnion = .{};
            var seed_offset: usize = 0;
            var text_offset: usize = 0;
            while (true) {
                var scratch = std.heap.ArenaAllocator.init(self.root.alloc);
                defer scratch.deinit();
                const page = scratch.allocator();
                var txn = try db.core.store.beginReadTxn();
                defer txn.abort();
                var bridge: Bridge = .{ .txn = &txn };
                const store = bridge.store(page, prefix);
                const head = try store.head(cfg.key_template) orelse return error.Busy;
                if (head.revision != expected.revision or !std.mem.eql(u8, &head.fingerprint, &expected.fingerprint)) return error.Busy;
                const seed_end = @min(seeds.count(), seed_offset + 1024);
                const text_end = @min(texts.count(), text_offset + 128);
                var part = if (prepared) |query| try store.queryExecutionPrepared(query, seeds.keys()[seed_offset..seed_end], texts.keys()[text_offset..text_end], candidates != null) else (try store.queryExecutionPolicy(cfg.key_template, seeds.keys()[seed_offset..seed_end], texts.keys()[text_offset..text_end], &.{}, candidates != null)) orelse return error.Busy;
                if (profile_lookup != null) try indexed_profile.projectSuppression(page, &part, seeds.keys()[seed_offset..seed_end], texts.keys()[text_offset..text_end]);
                try joined.add(a, page, part);
                seed_offset = seed_end;
                text_offset = text_end;
                if (seed_offset == seeds.count() and text_offset == texts.count()) break;
            }
            const snapshot = try joined.finish();
            complete_aliases = snapshot.complete_aliases;
            if (snapshot.configured_suppression) |suppression| {
                current.suppress_json = try json(a, suppression);
                complete_suppression = true;
            }
            current.merges = snapshot.namespace.merges;
            current.curation_json = try json(a, snapshot.namespace.state);
            current.shared_projection = null;
            current.validation_proof = null;
        } else {
            if (try namespaceStamp(db, a, cfg.key_template) != null) return error.Busy;
            current = try currentConfig(self.root, self.database, a, cfg.*, null);
        }
        if (captured.stamp != null and complete_aliases) {
            try target.installQueryProjection(a, if (complete_suppression) null else self.profiles.get(cfg.name), current.suppress_json, current.merges, current.curation_json, seeds.keys());
        } else if (captured.stamp != null) {
            try target.installQueryPolicy(a, self.profiles.get(cfg.name), cfg.aliases, cfg.suppress_json, current.merges, current.curation_json, seeds.keys());
        } else try target.installCachedPolicy(current.aliases, current.merges, current.suppress_json, current.curation_json);
    }
};

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
// An empty namespace must not publish a reverse-index marker and permanently
// fence old readers merely because automatic maintenance inspected it. Jobs
// can outlive their definitions, so check durable work before declaring clean.
fn projectionWorkPossible(database: *h.db_mod.DB, a: A) !bool {
    if (database.core.index_manager.hasResolverDefinitions()) return true;
    var scratch = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var txn = try database.core.store.beginReadTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    return (try bridge.store(temporary, "").backend.scan(&bridge, temporary, @import("../storage/lite/resolver_consumers.zig").work_prefix, null, 1)).rows.len != 0;
}

fn ownershipWorkPossible(database: *h.db_mod.DB, a: A) !bool {
    var scratch = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var txn = try database.core.store.beginReadTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    return try bridge.backendGet(temporary, marker) != null and !try bridge.store(temporary, prefix).ownershipIndexed();
}

pub fn hasProjectionDebt(root: *h.Handle, database: *h.db_mod.DB, a: A) !bool {
    const physical = if (root.owned_lite_backend) |*backend| backend.native_docstore else null;
    if (physical != null) {
        if (try queryPinsWorkPossible(database, a)) return true;
        if (try ownershipWorkPossible(database, a)) return true;
        if (!try projectionWorkPossible(database, a)) return false;
        if (database.core.index_manager.hasResolverDefinitions()) try ensureConsumerIndex(root, a);
        var read = try database.core.store.beginReadTxn();
        defer read.abort();
        var bridge: Bridge = .{ .txn = &read };
        if (try bridge.backendGet(a, marker) != null and !try bridge.store(a, prefix).ownershipIndexed()) return true;
        return (try bridge.store(a, "").backend.scan(&bridge, a, @import("../storage/lite/resolver_consumers.zig").work_prefix, null, 1)).rows.len != 0;
    }
    for (try database.core.index_manager.listResolverAdmissionDescriptors(a)) |cfg| if (try projection(root, a, cfg)) |desired| {
        if (desired.changed) return true;
    };
    return false;
}

test "capi resolver curation prepared adoption fences a source config edit" {
    const api = @import("db.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-prepared-adoption");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    const root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const handle = try api.publishHandle(root);
    defer api.antfly_db_close(handle);
    try tables.create(root, "entities", "{}", false);
    var cfg: h.db_mod.ResolverConfig = .{ .name = "r", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{_key}}", .promote = false };
    try root.db.addResolver(cfg);
    const db = try tables.get(root, "entities");
    const prepared = try prepareAdoption(a, db, try consumers(root, a, "entities", "concept/a", null));
    try std.testing.expectEqual(@as(usize, 1), prepared.namespaces.len);
    cfg.min_confidence = 0.5;
    _ = try root.db.upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false, .wait_for_activity = true });
    var txn = try db.core.store.beginWriteTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const store = bridge.store(a, prefix);
    try std.testing.expectError(error.ResolverCurationConflict, adopt(a, store, prepared));
    try std.testing.expect(try store.head(cfg.key_template) == null);
}

test "capi resolver curation indexed static profiles exclude seventeen thousand unrelated aliases after projection and reopen" {
    const api = @import("db.zig");
    const a0 = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a0);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-indexed-static-profiles");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    var root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    var handle = try api.publishHandle(root);
    defer api.antfly_db_close(handle);
    try tables.create(root, "entities", "{}", false);
    for (0..17) |i| {
        const static = try a.alloc(@import("antfly_resolver").KeyAlias, 1000);
        for (static, 0..) |*alias, j| alias.* = .{ .key = try std.fmt.allocPrint(a, "concept/noise-{d}-{d}", .{ i, j }), .target = try std.fmt.allocPrint(a, "concept/target-{d}-{d}", .{ i, j }) };
        if (i == 0) static[0] = .{ .key = "concept/seed", .target = "concept/bridge" };
        if (i == 1) static[0] = .{ .key = "concept/bridge", .target = "concept/end" };
        _ = try root.db.upsertResolverWithResultOptions(.{ .name = try std.fmt.allocPrint(a, "r-{d}", .{i}), .table = "entities", .source_artifact = try std.fmt.allocPrint(a, "kg-{d}", .{i}), .resolution_artifact = try std.fmt.allocPrint(a, "out-{d}", .{i}), .key_template = "concept/{{_key}}", .aliases = static, .promote = false }, .{ .drain_backfill = false, .wait_for_activity = true });
    }
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    // Cold adoption folds legacy policy once. Warm operations use adjacency.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_merge_json(handle, .fromSlice("entities"), .fromSlice("concept/new"), .fromSlice("{\"into\":\"concept/new-target\"}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    native.static_profiles.test_edge_visits.store(0, .monotonic);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_merge_json(handle, .fromSlice("entities"), .fromSlice("concept/seed"), .fromSlice("{\"into\":\"concept/final\",\"replace\":true}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expect(native.static_profiles.test_edge_visits.load(.monotonic) <= 8);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    api.antfly_db_close(handle);
    root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, false, null, null);
    handle = try api.publishHandle(root);
    native.static_profiles.test_edge_visits.store(0, .monotonic);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_unmerge_json(handle, .fromSlice("entities"), .fromSlice("concept/seed"), .fromSlice("{\"pin_distinct\":true}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expect(native.static_profiles.test_edge_visits.load(.monotonic) <= 8);
    try std.testing.expectEqualStrings("21", try root.db.core.store.get(a, catalog.minimum_reader_key));
}

test "capi resolver curation static directory fences consumers enrolled after discovery" {
    const api = @import("db.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-static-directory-fence");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    const root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const handle = try api.publishHandle(root);
    defer api.antfly_db_close(handle);
    try tables.create(root, "entities", "{}", false);
    var observed: ?u64 = null;
    const empty = try consumers(root, a, "entities", "concept/seed", &observed);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
    try checkDirectoryEpoch(root, observed);
    try tables.create(root, "late", "{}", false);
    const late = try tables.get(root, "late");
    try late.addResolver(.{ .name = "late", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{_key}}", .aliases = &.{.{ .key = "concept/seed", .target = "concept/blocked" }}, .promote = false });
    try std.testing.expectError(error.ResolverCurationConflict, checkDirectoryEpoch(root, observed));
}

test "capi resolver curation prepares shared profile slices once before consumer fanout" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const entries = try a.alloc(@import("antfly_resolver").KeyAlias, 1000);
    for (entries, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "entity/from-{d}", .{i}), .target = try std.fmt.allocPrint(a, "entity/to-{d}", .{i}) };
    const inputs = try a.alloc(Consumer, 10000);
    for (inputs) |*source| source.* = .{ .db = undefined, .source = "source", .cfg = .{ .name = "r", .table = "entities", .key_template = "entity/{{_key}}", .source_artifact = "kg", .resolution_artifact = "resolved", .admission_profile = "immutable-profile", .aliases = entries } };
    const selected = try prepareProfileCaches(a, inputs);
    for (selected) |source| {
        try std.testing.expect(source.profile_cache.? == selected[0].profile_cache.?);
        try std.testing.expectEqual(@as(usize, 1), source.profile_cache.?.get(source.cfg.key_template).?.len);
        try std.testing.expect(source.profile_cache.?.get(source.cfg.key_template).?[0].aliases.ptr == entries.ptr);
    }
}

test "capi resolver curation namespace profile contexts have constant allocation lookup" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const inputs = try a.alloc(Consumer, 10000);
    for (inputs, 0..) |*source, i| source.* = .{ .db = undefined, .source = "source", .cfg = .{ .name = "r", .table = "entities", .key_template = try std.fmt.allocPrint(a, "scope-{d}/{{{{_key}}}}", .{i}), .source_artifact = "kg", .resolution_artifact = "resolved", .admission_profile = try std.fmt.allocPrint(a, "profile-{d}", .{i}), .aliases = &.{} } };
    const selected = try prepareProfileCaches(a, inputs);
    try std.testing.expectEqual(@as(usize, 10000), selected[0].profile_cache.?.count());
    var bytes: [1]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&bytes);
    for (inputs) |source| {
        const profiles = try consumerProfiles(bounded.allocator(), selected, source.cfg.key_template);
        try std.testing.expectEqual(@as(usize, 1), profiles.len);
        try std.testing.expectEqualStrings(source.cfg.key_template, profiles[0].key_template);
    }
}

test "capi resolver curation schema digests qualify snapshots and reuse unchanged epochs" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("query-schema-digests");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    defer h.closeHandle(root);
    var stamp = try QuerySession.schema(&root.db, a);
    var bytes: [1]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&bytes);
    for (0..10000) |_| try stamp.verify(&root.db, bounded.allocator());
    root.db.core.index_manager.invalidateWritePlanSnapshot();
    try stamp.verify(&root.db, a);
    try stamp.verify(&root.db, bounded.allocator());
    // A new immutable snapshot may reuse identical scalar generation values.
    // Its process incarnation must still force content requalification.
    const prior_instance = stamp.instance;
    root.db.query_schema_identity.store(0, .release);
    try std.testing.expect(!stamp.matchesEpoch(&root.db));
    try stamp.verify(&root.db, bounded.allocator());
    try std.testing.expect(stamp.matchesEpoch(&root.db));
    try std.testing.expect(stamp.instance != prior_instance);
    const version: u32 = blk: {
        var view = root.db.core.acquireSchemaView() orelse break :blk 0;
        defer view.release();
        break :blk view.tableSchema().version;
    };
    const update = try json(a, .{ .version = version + 1, .default_type = "document", .document_schemas = .{ .document = .{ .schema = .{ .type = "object", .properties = .{ .title = .{ .type = "string" } } } } } });
    defer a.free(update);
    try root.db.setSchemaJson(a, update);
    try std.testing.expectError(error.Busy, stamp.verify(&root.db, a));
}

test "capi resolver curation proposal review unions queue work across namespaces and samples each source once" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = try api.TestDirectory.init("review-batched-namespaces");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    defer h.closeHandle(root);
    try tables.create(root, "entities", "{}", false);
    const db = try tables.get(root, "entities");
    const scope_count = 32;
    for (0..scope_count) |i| {
        const spaces = try scratch.alloc(u8, i + 1);
        @memset(spaces, ' ');
        try root.db.addResolver(.{ .name = try std.fmt.allocPrint(scratch, "r-{d}", .{i}), .table = "entities", .source_artifact = try std.fmt.allocPrint(scratch, "kg-{d}", .{i}), .resolution_artifact = try std.fmt.allocPrint(scratch, "resolved-{d}", .{i}), .key_template = try std.fmt.allocPrint(scratch, "entity/{{{{{s}_key}}}}", .{spaces}), .promote = false });
    }
    _ = try entity(root, scratch, "entities", "entity/a", .{ .action = .unsuppress }, .{});
    const entries = try scratch.alloc(c.Proposal, 64);
    for (entries, 0..) |*entry, i| {
        const input: c.ProposalRequest = .{ .action = .merge, .key = "entity/a", .into = try std.fmt.allocPrint(scratch, "entity/target-{d}", .{i}) };
        const note = if (i == 0) large: {
            const text = try scratch.alloc(u8, 14000);
            @memset(text, 'x');
            break :large text;
        } else "retained evidence";
        entry.* = .{ .id = try c.proposalId(scratch, input), .action = input.action, .key = input.key, .into = input.into, .origin = .{}, .evidence = try value(scratch, .{ .note = note }) };
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        var bridge: Bridge = .{ .txn = &txn };
        const store = bridge.store(scratch, prefix);
        const head = try store.queueHead();
        try store.applyQueue(.{ .expected_revision = head.revision, .revision = head.revision + 1, .receipt_scope = @splat('a'), .proposals = entries });
        try bridge.flush(scratch);
        try txn.commit();
    }
    const before = native.test_proposal_key_scans.load(.monotonic);
    const arena_before = arena.queryCapacity();
    const bytes = try proposals(root, scratch, .decide, "entities", entries[0].id, "{\"accept\":true}", .{});
    const scans = native.test_proposal_key_scans.load(.monotonic) - before;
    const result = try std.json.parseFromSliceLeaky(std.json.Value, scratch, bytes, .{});
    try std.testing.expectEqual(@as(usize, scope_count), result.object.get("namespaces").?.array.items.len);
    try std.testing.expect(scans <= 3);
    var txn = try db.core.store.beginReadTxn();
    defer txn.abort();
    var bridge: Bridge = .{ .txn = &txn };
    const store = bridge.store(scratch, prefix);
    for (entries, 0..) |entry, i| {
        const current = (try store.proposal(entry.id)).?;
        try std.testing.expectEqual(if (i == 0) c.ProposalState.accepted else c.ProposalState.superseded, current.state);
        try std.testing.expectEqualStrings(entry.evidence.?.object.get("note").?.string, current.evidence.?.object.get("note").?.string);
    }
    var observations: ReplayObservations = .{};
    for (0..1000) |i| _ = try observations.applied(scratch, .{ .db = &root.db, .source = "default", .cfg = .{ .name = "r", .table = "entities", .source_artifact = "", .resolution_artifact = "", .key_template = "entity/{{_key}}", .promote = i % 2 == 0 } });
    try std.testing.expectEqual(@as(usize, 1), observations.values.count());
    try std.testing.expect(observations.values.get(&root.db).? != null);
    std.debug.print("REVIEW namespaces={d} proposals={d} incidence_scans={d} source_samples={d} arena_added={d}\n", .{ scope_count, entries.len, scans, observations.values.count(), arena.queryCapacity() - arena_before });
}

test "capi resolver curation warm proposal creation skips all consumer bindings and fences membership" {
    const api = @import("db.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("proposal-membership-only");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    const root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    defer h.closeHandle(root);
    try tables.create(root, "entities", "{}", false);
    const db = try tables.get(root, "entities");
    for (0..100) |i| {
        _ = try root.db.upsertResolverWithResultOptions(.{ .name = try std.fmt.allocPrint(a, "r-{d}", .{i}), .table = "entities", .key_template = "entity/{{_key}}", .source_artifact = try std.fmt.allocPrint(a, "kg-{d}", .{i}), .resolution_artifact = try std.fmt.allocPrint(a, "resolved-{d}", .{i}), .promote = false }, .{ .drain_backfill = false, .wait_for_activity = true });
    }
    // First proposal folds cold policy; every following proposal is membership-only.
    _ = try proposals(root, a, .create, "entities", "", "{\"action\":\"merge\",\"key\":\"entity/a\",\"into\":\"entity/b\"}", .{});
    const visits = test_admission_binding_visits.load(.monotonic);
    for (0..20) |i| _ = try proposals(root, a, .create, "entities", "", try json(a, c.ProposalRequest{ .action = .merge, .key = "entity/a", .into = try std.fmt.allocPrint(a, "entity/target-{d}", .{i}) }), .{});
    try std.testing.expectEqual(visits, test_admission_binding_visits.load(.monotonic));
    var observed: ?u64 = null;
    const selected = try consumersForAdmission(root, a, "entities", "entity/a", &observed, db);
    try std.testing.expectEqual(@as(usize, 1), selected.len);
    try std.testing.expect(selected[0].synthetic and selected[0].scope_head != null);
    try std.testing.expectEqual(@as(usize, 0), (try prepareAdoption(a, db, selected)).sources.len);
    _ = try entity(root, a, "entities", "entity/a", .{ .action = .suppress }, .{});
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var bridge: Bridge = .{ .txn = &read };
        try std.testing.expectError(error.ResolverCurationConflict, checkScopeHeads(a, bridge.store(a, prefix), selected));
    }
    _ = try root.db.upsertResolverWithResultOptions(.{ .name = "late", .table = "entities", .key_template = "entity/{{_key}}", .source_artifact = "late-kg", .resolution_artifact = "late-resolved", .promote = false }, .{ .drain_backfill = false, .wait_for_activity = true });
    try std.testing.expectError(error.ResolverCurationConflict, checkDirectoryEpoch(root, observed));
    // Mixed warm/cold membership folds just the newly enrolled namespace.
    _ = try root.db.upsertResolverWithResultOptions(.{ .name = "cold", .table = "entities", .key_template = "{{_key}}", .source_artifact = "cold-kg", .resolution_artifact = "cold-resolved", .promote = false }, .{ .drain_backfill = false, .wait_for_activity = true });
    const cold_visits = test_admission_binding_visits.load(.monotonic);
    const mixed = try consumersForAdmission(root, a, "entities", "entity/a", &observed, db);
    try std.testing.expectEqual(@as(usize, 2), mixed.len);
    const prepared = try prepareAdoption(a, db, mixed);
    try std.testing.expectEqual(@as(usize, 1), prepared.sources.len);
    try std.testing.expectEqual(@as(usize, 1), prepared.namespaces.len);
    try std.testing.expectEqualStrings("{{_key}}", prepared.namespaces[0].key_template);
    try std.testing.expectEqual(@as(usize, 1), test_admission_binding_visits.load(.monotonic) - cold_visits);
    _ = try proposals(root, a, .create, "entities", "", "{\"action\":\"merge\",\"key\":\"entity/a\",\"into\":\"entity/cold\"}", .{});
    std.debug.print("PROPOSALS consumers=100 creations=20 warm_binding_visits=0 mixed_cold_visits=1\n", .{});
}

test "capi resolver curation owner maintenance backfills historical ownership in bounded durable turns" {
    const api = @import("db.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("ownership-owner-backfill");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    var root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    var handle = try api.publishHandle(root);
    defer api.antfly_db_close(handle);
    try tables.create(root, "entities", "{}", false);
    try root.db.addResolver(.{ .name = "r", .table = "entities", .key_template = "legacy/{{_key}}", .source_artifact = "kg", .resolution_artifact = "resolved", .promote = false });
    _ = try entity(root, a, "entities", "legacy/seed", .{ .action = .unsuppress }, .{});
    var db = try tables.get(root, "entities");
    const values = try a.alloc(c.Proposal, 300);
    for (values, 0..) |*value0, i| {
        const proposed = try c.propose(a, .{}, &.{}, .{ .action = .suppress, .key = try std.fmt.allocPrint(a, "legacy/{d}", .{i}) }, .{});
        value0.* = proposed.state.proposals[0];
        value0.state = .rejected;
        value0.decided = .{};
    }
    {
        var write = try db.core.store.beginWriteTxn();
        errdefer write.abort();
        var bridge: Bridge = .{ .txn = &write };
        const store = bridge.store(a, prefix);
        try store.applyQueue(.{ .expected_revision = 0, .revision = 1, .proposals = values });
        const owned = try std.mem.concat(a, u8, &.{ prefix, "queue/ownership/" });
        var after: ?[]const u8 = null;
        while (true) {
            const page = try store.backend.scan(&bridge, a, owned, after, 128);
            for (page.rows) |row| try store.backend.remove(&bridge, a, row.key);
            after = page.next;
            if (after == null) break;
        }
        try store.backend.remove(&bridge, a, try std.mem.concat(a, u8, &.{ prefix, "queue/ownership-progress" }));
        try bridge.backendPut(a, catalog.minimum_reader_key, try catalog.readerVersionBytes(catalog.resolver_projection_delta_reader_version));
        try bridge.flush(a);
        try write.commit();
    }
    db.core.index_manager.minimum_reader_version.store(catalog.resolver_projection_delta_reader_version, .release);
    try std.testing.expect(try hasProjectionDebt(root, db, a));
    try std.testing.expect(try owns(db, a, "legacy/2{{_key}}"));
    var turns: usize = 0;
    while (try hasProjectionDebt(root, db, a)) {
        try std.testing.expect(turns < 4);
        try std.testing.expect(try pullProjection(root, db, a));
        turns += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), turns);
    try std.testing.expectEqual(catalog.resolver_projection_delta_reader_version, db.core.index_manager.minimum_reader_version.load(.acquire));
    try std.testing.expectEqualStrings("15", try db.core.store.get(a, catalog.minimum_reader_key));
    api.antfly_db_close(handle);
    root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, false, null, null);
    handle = try api.publishHandle(root);
    db = try tables.get(root, "entities");
    try std.testing.expect(!try hasProjectionDebt(root, db, a));
    try std.testing.expect(try owns(db, a, "legacy/2{{_key}}"));
    try std.testing.expect(!try owns(db, a, "unrelated/{{_key}}"));
    std.debug.print("OWNERSHIP LITE historical_keys=300 turns={d} reopen=indexed\n", .{turns});
}

test "capi resolver curation query uses persisted profile nodes under leased cache pressure" {
    const resolver = @import("antfly_resolver");
    const api = @import("db.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-query-admission");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    const root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const handle = try api.publishHandle(root);
    defer api.antfly_db_close(handle);
    try tables.create(root, "entities", "{}", false);
    const profile_aliases = try a.alloc(resolver.KeyAlias, 5000);
    for (profile_aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "concept/query-admission-{d}", .{i}), .target = "concept/query-root" };
    const negatives = try a.alloc([]const u8, 10000);
    for (negatives, 0..) |*key, i| key.* = try std.fmt.allocPrint(a, "concept/blocked-{d}", .{i});
    negatives[0] = "concept/query-admission-0";
    const suppress = try json(a, .{ .keys = negatives, .texts = &.{" BLOCKED TEXT "}, .min_text_chars = @as(u32, 3) });
    const cfg: h.db_mod.ResolverConfig = .{ .name = "query-admission", .table = "entities", .source_artifact = "kg", .resolution_artifact = "out", .key_template = "concept/{{_entity.text}}", .aliases = profile_aliases, .suppress_json = suppress, .promote = false };
    _ = try root.db.upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false, .wait_for_activity = true });
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_merge_json(handle, .fromSlice("entities"), .fromSlice("concept/native"), .fromSlice("{\"into\":\"concept/native-root\"}"), &out));
    const Pressure = struct {
        fn load(_: *anyopaque, alloc: A) !native.query_context.Graph {
            const bytes = try alloc.alloc(u8, 28 * 1024 * 1024);
            @memset(bytes, 'x');
            return .{ .shared_profile = true, .routes = try alloc.dupe(native.query_context.Route, &.{.{ .root = bytes, .component = "pressure" }}) };
        }
    };
    var unused: u8 = 0;
    const pressure = try native.query_context.acquire(@splat(210), true, .{ .ptr = &unused, .load = Pressure.load });
    defer pressure.release();
    const admission = try root.db.core.index_manager.resolverQueryAdmission(a, cfg.name, true);
    defer admission.deinit();
    const installed = admission.configs;
    const query_cfg = installed[0];
    var session = try QuerySession.initAdmitted(a, root, query_cfg, &admission);
    var target = try resolver.Resolver.initFromParts(a, cfg.table, cfg.key_template, .{}, false, "");
    defer target.deinit();
    const mentions: []const resolver.ExtractedEntity = &.{.{ .local_id = "query-admission-0", .label = "concept", .text = "query-admission-0" }};
    const builds = native.test_query_base_loads.load(.monotonic);
    try QuerySession.prepare(&session, &query_cfg, a, &target, mentions, null);
    try std.testing.expectEqual(builds, native.test_query_base_loads.load(.monotonic));
    try std.testing.expectEqualStrings("concept/query-root", target.alias_index.?.canonical(profile_aliases[0].key));
    try std.testing.expectEqual(@as(usize, 0), target.query_policy_lease.?.aliases().len);
    try std.testing.expect(target.curation_index.keySuppressed("concept/query-admission-0"));
    try std.testing.expect(target.query_policy_lease.?.policy.bytes < 16384);
    try QuerySession.prepare(&session, &query_cfg, a, &target, &.{.{ .local_id = "allowed", .label = "concept", .text = "allowed" }}, null);
    try std.testing.expect(!target.curation_index.keySuppressed("concept/allowed"));
    try QuerySession.prepare(&session, &query_cfg, a, &target, &.{.{ .local_id = "blocked", .label = "concept", .text = "blocked text" }}, null);
    try std.testing.expect((try target.curation_index.textReason(a, " Blocked Text ")) != null);
    try session.verify(root);
}

test "capi resolver curation final declaration fence excludes unrelated profiles and tracks scoped edits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Manager = @import("../storage/db/catalog/index_manager.zig").IndexManager;
    var manager: Manager = undefined;
    manager.alloc = a;
    manager.catalog_mutex = .{};
    manager.resolvers = .empty;
    manager.retired_resolvers = .empty;
    manager.resolver_namespace_lookup = null;
    manager.test_resolver_lookup_builds = 0;
    manager.test_resolver_query_proof_builds = 0;
    manager.write_plan_generation = .init(1);
    defer if (manager.resolver_namespace_lookup) |*lookup| lookup.arena.deinit();
    const selected: h.db_mod.ResolverConfig = .{ .name = "selected", .table = "entities", .key_template = "entity/{{_entity.text}}", .source_artifact = "selected-kg", .resolution_artifact = "out" };
    try manager.resolvers.append(a, selected);
    const declared = try a.alloc(@import("antfly_resolver").KeyAlias, 1000);
    for (declared, 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "other/alias-{d}", .{i}), .target = "other/root" };
    for (1..1000) |i| try manager.resolvers.append(a, .{ .name = try std.fmt.allocPrint(a, "unrelated-{d}", .{i}), .table = "other-entities", .key_template = "other/{{_entity.text}}", .source_artifact = "other-kg", .resolution_artifact = "other-out", .aliases = declared });
    const expected = try QuerySession.declarations(a, manager.resolvers.items, selected);
    try std.testing.expectEqualSlices(u8, &expected, &try manager.resolverQueryDeclarationsFingerprint(selected));
    for (0..1000) |_| try std.testing.expectEqualSlices(u8, &expected, &try manager.resolverQueryDeclarationsFingerprint(selected));
    try std.testing.expectEqual(@as(usize, 1), manager.test_resolver_query_proof_builds);
    manager.resolvers.items[0].min_confidence = 0.8;
    manager.invalidateWritePlanSnapshot();
    const changed = try manager.resolverQueryDeclarationsFingerprint(selected);
    try std.testing.expect(!std.mem.eql(u8, &expected, &changed));
    try std.testing.expectEqualSlices(u8, &try QuerySession.declarations(a, manager.resolvers.items, selected), &changed);
    manager.resolvers.items[1].source_artifact = "selected-kg";
    manager.invalidateWritePlanSnapshot();
    const sibling = try manager.resolverQueryDeclarationsFingerprint(selected);
    try std.testing.expect(!std.mem.eql(u8, &changed, &sibling));
    manager.resolvers.items[2].min_confidence = 0.7;
    manager.invalidateWritePlanSnapshot();
    try std.testing.expectEqualSlices(u8, &sibling, &try manager.resolverQueryDeclarationsFingerprint(selected));
    std.debug.print("FINAL DECLARATION FENCE resolvers=1000 unrelated_aliases=999000 warm_checks=1000 proof_builds=1 descriptor_copies=0\n", .{});
}

test "capi resolver curation initial routing admission excludes unrelated policy and preserves sibling fences" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Manager = @import("../storage/db/catalog/index_manager.zig").IndexManager;
    var manager: Manager = undefined;
    manager.alloc = a;
    manager.catalog_mutex = .{};
    manager.resolvers = .empty;
    manager.retired_resolvers = .empty;
    manager.resolver_namespace_lookup = null;
    manager.test_resolver_lookup_builds = 0;
    manager.test_resolver_query_proof_builds = 0;
    manager.write_plan_generation = .init(1);
    defer if (manager.resolver_namespace_lookup) |*lookup| lookup.arena.deinit();
    const selected: h.db_mod.ResolverConfig = .{ .name = "selected", .table = "entities", .key_template = "entity/{{_entity.text}}", .source_artifact = "selected-kg", .resolution_artifact = "out" };
    try manager.resolvers.append(a, selected);
    var sibling = selected;
    sibling.name = "sibling";
    sibling.labels = &.{"person"};
    sibling.aliases = &.{.{ .key = "person/ada", .target = "person/lovelace" }};
    try manager.resolvers.append(a, sibling);
    var chunk = selected;
    chunk.name = "chunk";
    chunk.source_artifact_kind = .chunk;
    try manager.resolvers.append(a, chunk);
    var staged = selected;
    staged.name = "staged";
    staged.replacement_index = "graph-v2";
    try manager.resolvers.append(a, staged);
    var retired = selected;
    retired.name = "retired";
    retired.retired = true;
    try manager.retired_resolvers.append(a, retired);
    const declared = try a.alloc(@import("antfly_resolver").KeyAlias, 1000);
    for (declared, 0..) |*edge, i| edge.* = .{ .key = try std.fmt.allocPrint(a, "other/alias-{d}", .{i}), .target = "other/root" };
    for (0..999) |i| try manager.resolvers.append(a, .{ .name = try std.fmt.allocPrint(a, "unrelated-{d}", .{i}), .table = "other-entities", .key_template = "other/{{_entity.text}}", .source_artifact = "other-kg", .resolution_artifact = "other-out", .aliases = declared });
    for (0..10) |_| {
        var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer scratch.deinit();
        const admission = try manager.resolverQueryAdmission(scratch.allocator(), selected.name, true);
        defer admission.deinit();
        const configs = admission.configs;
        try std.testing.expectEqual(@as(usize, 2), configs.len);
        try std.testing.expectEqualStrings("selected", configs[0].name);
        try std.testing.expectEqualStrings("sibling", configs[1].name);
        try std.testing.expectEqualStrings("person/lovelace", configs[1].aliases[0].target);
        const labels = try QuerySession.runtime.siblingClaimedLabelsAlloc(scratch.allocator(), configs, &configs[0]);
        try std.testing.expectEqualStrings("person", labels[0]);
        const expected = try QuerySession.declarations(a, manager.resolvers.items, selected);
        try std.testing.expectEqualSlices(u8, &expected, &try QuerySession.declarations(a, configs, selected));
        try std.testing.expectEqualSlices(u8, &expected, &try manager.resolverQueryDeclarationsFingerprint(selected));
        try std.testing.expect(scratch.queryCapacity() < 16 * 1024);
    }
    try std.testing.expectEqual(@as(usize, 1), manager.test_resolver_lookup_builds);
    for ([_][]const u8{ "missing", "retired", "staged" }) |name| {
        const admission = try manager.resolverQueryAdmission(a, name, true);
        defer admission.deinit();
        try std.testing.expectEqual(@as(usize, 0), admission.configs.len);
    }
    const private_admission = try manager.resolverQueryAdmission(a, "staged", false);
    defer private_admission.deinit();
    const private_configs = private_admission.configs;
    try std.testing.expectEqual(@as(usize, 1), private_configs.len);
    try std.testing.expectEqualStrings("staged", private_configs[0].name);
    try std.testing.expectEqualSlices(u8, &private_admission.fingerprint, &try manager.resolverQueryDeclarationsFingerprint(private_configs[0]));
    // Snapshots own the declarations they admitted; a later sibling edit must
    // change the final proof without changing an already captured snapshot.
    const captured = try manager.resolverQueryAdmission(a, selected.name, true);
    defer captured.deinit();
    const snapshot = captured.configs;
    const before = try QuerySession.declarations(a, snapshot, selected);
    manager.resolvers.items[1].min_confidence = 0.8;
    manager.invalidateWritePlanSnapshot();
    try std.testing.expect(!std.mem.eql(u8, &before, &try manager.resolverQueryDeclarationsFingerprint(selected)));
    try std.testing.expect(snapshot[1].min_confidence == null);
    std.debug.print("INITIAL ROUTING ADMISSION unrelated_aliases=999000 returned=2 warm_requests=10 arena_bytes_per_request<16384 lookup_builds=1\n", .{});
}

test "capi resolver curation schema captures reuse owner proofs across large unrelated catalogs" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("query-schema-owner-proof");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    defer h.closeHandle(root);
    const manager = root.db.core.index_manager;
    const text = try a.alloc(u8, 20000);
    defer a.free(text);
    @memset(text, 'x');
    const Enrichment = @import("../storage/db/catalog/enrichment_catalog.zig").EnrichmentConfig;
    for (0..1000) |i| {
        const name = try std.fmt.allocPrint(a, "unrelated-{d}", .{i});
        defer a.free(name);
        var owned = try Enrichment.clone(manager.alloc, .{ .name = name, .kind = .embedding, .source_field = "body", .source_template = text });
        errdefer owned.deinit(manager.alloc);
        try manager.enrichments.append(manager.alloc, owned);
    }
    manager.invalidateWritePlanSnapshot();
    var captured = try QuerySession.schema(&root.db, a);
    var bytes: [1]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&bytes);
    for (0..1000) |_| {
        const warm = try QuerySession.schema(&root.db, bounded.allocator());
        try std.testing.expectEqualSlices(u8, &captured.digest, &warm.digest);
    }
    // Catalog replacement requalifies content once; cached content cannot
    // mask an enrichment change, even when schema version stays identical.
    manager.enrichments.items[0].full_text_index = true;
    manager.invalidateWritePlanSnapshot();
    try std.testing.expectError(error.Busy, captured.verify(&root.db, a));
    const updated = try QuerySession.schema(&root.db, bounded.allocator());
    try std.testing.expect(!std.mem.eql(u8, &captured.digest, &updated.digest));
    root.db.query_schema_identity.store(0, .release);
    try std.testing.expect(!updated.matchesEpoch(&root.db));
    try std.testing.expectError(error.OutOfMemory, QuerySession.schema(&root.db, bounded.allocator()));
    const reopened = try QuerySession.schema(&root.db, a);
    try std.testing.expectEqualSlices(u8, &updated.digest, &reopened.digest);
    try std.testing.expect(updated.instance != reopened.instance);
    std.debug.print("OWNER SCHEMA PROOF templates=1000 bytes=20000000 warm_captures=1000 scratch_bytes=0 content_and_incarnation=fenced\n", .{});
}

test "capi resolver curation owner materializes graph fenced root pins and reopens without cache preparation" {
    const api = @import("db.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-root-pin-owner");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    var root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    var handle = try api.publishHandle(root);
    defer api.antfly_db_close(handle);
    try tables.create(root, "entities", "{}", false);
    const cfg: h.db_mod.ResolverConfig = .{ .name = "r", .table = "entities", .key_template = "entity/{{_key}}", .source_artifact = "kg", .resolution_artifact = "resolved", .promote = false, .aliases = &.{.{ .key = "entity/alias", .target = "entity/root" }} };
    try root.db.addResolver(cfg);
    const id = try (native.static_profiles.Definition{ .key_template = cfg.key_template, .aliases = cfg.aliases }).identity(a);
    _ = try entity(root, a, "entities", "entity/native", .{ .action = .merge, .into = "entity/root" }, .{});
    _ = try entity(root, a, "entities", "entity/root", .{ .action = .distinct, .into = "entity/other" }, .{});
    for (0..3) |phase| {
        if (phase == 2) _ = try entity(root, a, "entities", "entity/native", .{ .action = .unmerge }, .{});
        if (phase == 1) {
            api.antfly_db_close(handle);
            root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, false, null, null);
            handle = try api.publishHandle(root);
        }
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
        const destination = try tables.get(root, "entities");
        var read = try destination.core.store.beginReadTxn();
        defer read.abort();
        var bridge: Bridge = .{ .txn = &read };
        const authority = bridge.store(a, prefix);
        try std.testing.expect(try authority.queryPinsIndexed(cfg.key_template, id));
        const file = root.owned_lite_backend.?.native_docstore.?;
        var physical = try file.beginRead();
        defer physical.abort();
        var directory: native.static_profiles.ReadTransaction(@TypeOf(physical)) = .{ .txn = &physical };
        var reader: native.static_profiles.QueryReader = .{ .backend = directory.backend(), .root = try @import("../storage/lite/resolver_consumers.zig").profileRoot(a, cfg.table, cfg.key_template), .profile = id, .indexed = true };
        const prepared = (try authority.prepareQuery(cfg.key_template, .{ .id = id, .lookup = reader.lookup() }, true)).?;
        defer prepared.release();
        const page = try authority.queryExecutionPrepared(prepared, &.{"entity/root"}, &.{}, true);
        try std.testing.expectEqual(@as(usize, 1), page.namespace.state.distinct.len);
        try std.testing.expectEqualStrings("21", try root.db.core.store.get(a, catalog.minimum_reader_key));
    }
}
