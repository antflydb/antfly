// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Independently compiled private candidate for regular CREATE UNIQUE INDEX.
//! This validates an index-only schema delta, not publication authority. The
//! controller must separately bind the complete owner cut and name/table locks.
const std = @import("std");
const schema_api = @import("../../schema/mod.zig");
const native_schema = @import("../schema.zig");
const native_index = @import("../relational_index.zig");
const rows = @import("algebraic/relational_row_codec.zig");
const tuples = @import("relational_index_keys.zig");
const predicates = @import("relational_index_predicate.zig");
const catalog = @import("relational_integrity_catalog.zig");
const compiler = @import("../../schema/relational_declarations.zig");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;
pub const max_schema_bytes = 2 * 1024 * 1024;

pub const Candidate = struct {
    next: catalog.Update,
    reservation: catalog.Update,
    definition_digest: [32]u8,

    pub fn deinit(self: *Candidate) void {
        self.reservation.deinit();
        self.next.deinit();
        self.* = undefined;
    }

    /// Owns both encoded catalogs. All validation precedes any store writes.
    /// The predecessor is the actual owner catalog, including retired bindings
    /// and burned generation allocations, not a reconstruction from schema.
    pub fn prepare(alloc: Allocator, table_id: u64, before_json: []const u8, after_json: []const u8, predecessor: []const u8, name: []const u8) !Candidate {
        if (table_id == 0 or name.len == 0 or name.len > 1024 or
            before_json.len > max_schema_bytes or after_json.len > max_schema_bytes or
            predecessor.len > catalog.max_catalog_bytes) return error.InvalidIndexPublicationCandidate;
        try validateSchemaDelta(alloc, before_json, after_json, name);
        var before = try schema_api.parseValidatedTableSchema(alloc, before_json);
        defer before.deinit(alloc);
        var after = try schema_api.parseValidatedTableSchema(alloc, after_json);
        defer after.deinit(alloc);
        if (before.storage_mode != .relational or after.storage_mode != .relational or
            before.external_base_source != null or after.external_base_source != null or
            after.version != (std.math.add(u32, before.version, 1) catch return error.InvalidIndexPublicationCandidate))
            return error.InvalidIndexPublicationCandidate;
        const definition_digest = try validatePhysicalPair(alloc, after, name);
        var current = try compiler.compileCatalog(alloc, before, table_id, predecessor);
        defer current.deinit();
        if (!std.mem.eql(u8, current.value, predecessor)) return error.IntegrityCatalogChanged;
        var next = try compiler.compileCatalog(alloc, after, table_id, predecessor);
        errdefer next.deinit();
        _ = try catalog.indexAddition(current.catalog, next.catalog, name);
        const reservation = try catalog.prepareIndexReservation(alloc, predecessor, next.catalog, name);
        return .{ .next = next, .reservation = reservation, .definition_digest = definition_digest };
    }
};

/// Name equality alone must not authorize building different physical keys or
/// partial membership than the UNIQUE rule being published. Compare independently
/// compiled equality keys/predicates; sort direction and covering columns are
/// presentation/storage choices, not uniqueness semantics.
fn validatePhysicalPair(alloc: Allocator, schema: schema_api.ParsedTableSchema, name: []const u8) ![32]u8 {
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const a = scratch.allocator();
    const indexes = (try schema.relationalIndexDefinitions(a)) orelse return error.InvalidIndexPublicationCandidate;
    const rules = try schema.relationalUniqueDefinitions(a);
    const index = for (indexes) |entry| {
        if (std.mem.eql(u8, entry.name, name)) break entry;
    } else return error.InvalidIndexPublicationCandidate;
    const rule = for (rules) |entry| {
        if (std.mem.eql(u8, entry.name, name)) break entry;
    } else return error.InvalidIndexPublicationCandidate;
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, schema);
    defer native_schema.freeSchema(alloc, runtime);
    var layout = try rows.PhysicalLayout.init(alloc, runtime);
    defer layout.deinit();
    const rule_keys = if (rule.keys.len != 0) rule.keys else blk: {
        const keys = try a.alloc(native_index.RelationalIndexKey, rule.columns.len);
        for (rule.columns, keys) |column, *key| key.* = .{ .column = column };
        break :blk keys;
    };
    var physical = try tuples.TuplePlan.init(alloc, runtime, &layout, index.keys);
    defer physical.deinit();
    var logical = try tuples.TuplePlan.init(alloc, runtime, &layout, rule_keys);
    defer logical.deinit();
    if (physical.keys.len != logical.keys.len) return error.InvalidIndexPublicationCandidate;
    for (0..physical.keys.len) |i| if (!physical.sameEqualityKey(i, logical, i)) return error.InvalidIndexPublicationCandidate;
    const physical_where = try predicateIdentity(alloc, runtime, &layout, index.where);
    const logical_where = try predicateIdentity(alloc, runtime, &layout, rule.where);
    if (!std.mem.eql(u8, &physical_where, &logical_where)) return error.InvalidIndexPublicationCandidate;
    return native_index.definitionDigest(index);
}

fn predicateIdentity(alloc: Allocator, schema: native_schema.TableSchema, layout: *const rows.PhysicalLayout, conditions: []const native_index.UniquePredicate) ![32]u8 {
    if (conditions.len == 0) return @splat(0);
    var plan = try predicates.Plan.init(alloc, schema, layout, conditions);
    defer plan.deinit();
    return plan.identity;
}

fn touched(key: []const u8) bool {
    return std.mem.eql(u8, key, "version") or std.mem.eql(u8, key, "relational_indexes") or std.mem.eql(u8, key, "unique_constraints");
}

const equal = @import("../../schema/json_shape.zig").equal;

fn entries(schema: Value, key: []const u8) ![]const Value {
    const value = schema.object.get(key) orelse return &.{};
    if (value != .array) return error.InvalidIndexPublicationCandidate;
    return value.array.items;
}

fn named(value: Value, name: []const u8) bool {
    if (value != .object) return false;
    const field = value.object.get("name") orelse return false;
    return field == .string and std.mem.eql(u8, field.string, name);
}

fn addition(before: []const Value, after: []const Value, name: []const u8) !Value {
    if (after.len != before.len + 1) return error.InvalidIndexPublicationCandidate;
    for (before) |entry| if (named(entry, name)) return error.InvalidIndexPublicationCandidate;
    var old: usize = 0;
    var result: ?Value = null;
    for (after) |entry| {
        if (named(entry, name)) {
            if (result != null) return error.InvalidIndexPublicationCandidate;
            result = entry;
        } else {
            if (old == before.len or !equal(before[old], entry, 0)) return error.InvalidIndexPublicationCandidate;
            old += 1;
        }
    }
    if (old != before.len) return error.InvalidIndexPublicationCandidate;
    return result orelse error.InvalidIndexPublicationCandidate;
}

fn validateSchemaDelta(alloc: Allocator, before_json: []const u8, after_json: []const u8, name: []const u8) !void {
    var before = try std.json.parseFromSlice(Value, alloc, before_json, .{ .max_value_len = max_schema_bytes });
    defer before.deinit();
    var after = try std.json.parseFromSlice(Value, alloc, after_json, .{ .max_value_len = max_schema_bytes });
    defer after.deinit();
    if (before.value != .object or after.value != .object) return error.InvalidIndexPublicationCandidate;
    for ([_]bool{ false, true }) |reverse| {
        const left = if (reverse) after.value else before.value;
        const right = if (reverse) before.value else after.value;
        var it = left.object.iterator();
        while (it.next()) |entry| {
            if (touched(entry.key_ptr.*)) continue;
            const other = right.object.get(entry.key_ptr.*) orelse return error.InvalidIndexPublicationCandidate;
            if (!equal(entry.value_ptr.*, other, 0)) return error.InvalidIndexPublicationCandidate;
        }
    }
    _ = try addition(try entries(before.value, "relational_indexes"), try entries(after.value, "relational_indexes"), name);
    const rule = try addition(try entries(before.value, "unique_constraints"), try entries(after.value, "unique_constraints"), name);
    const origin = rule.object.get("origin") orelse return error.InvalidIndexPublicationCandidate;
    if (origin != .string or !std.mem.eql(u8, origin.string, "index")) return error.InvalidIndexPublicationCandidate;
    // An ordinary SQL index cannot smuggle a PRIMARY/deferred constraint into
    // the candidate. The typed schema compiler checks expression/key coverage.
    for ([_][]const u8{ "primary", "deferrable" }) |key| if (rule.object.get(key)) |value| {
        if (value != .bool or value.bool) return error.InvalidIndexPublicationCandidate;
    };
}

const test_before =
    \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}}}
;

test "relational index candidate owns its native catalogs after request memory is released" {
    const a = std.testing.allocator;
    var schema = try schema_api.parseValidatedTableSchema(a, test_before);
    defer schema.deinit(a);
    var prior = try compiler.compileCatalog(a, schema, 101, null);
    defer prior.deinit();
    var candidate = blk: {
        var request = std.heap.ArenaAllocator.init(a);
        defer request.deinit();
        const input = request.allocator();
        break :blk try Candidate.prepare(a, 101, try input.dupe(u8, test_before), try input.dupe(u8, test_after), try input.dupe(u8, prior.value), try input.dupe(u8, "email_idx"));
    };
    defer candidate.deinit();
    try std.testing.expectEqualStrings("email_idx", candidate.next.catalog.find(.unique, "email_idx").?.definition.name);
    try std.testing.expectEqualSlices(u8, prior.value, candidate.reservation.expected.?);
    try std.testing.expectEqual(@as(u32, 1), candidate.reservation.catalog.schema_version);
    try std.testing.expectEqual(@as(u32, 2), candidate.next.catalog.schema_version);
    var publication = try catalog.prepareIndexPublication(a, prior.value, candidate.reservation.value, candidate.next.value, "email_idx");
    defer publication.deinit();
    try std.testing.expectEqualSlices(u8, candidate.next.value, publication.value);
}
const test_after =
    \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}},"relational_indexes":[{"name":"email_idx","keys":[{"column":"email"}]}],"unique_constraints":[{"name":"email_idx","origin":"index","columns":["email"]}]}
;

fn testOwnerJobAdmission(comptime Backend: type) !void {
    const a = std.testing.allocator;
    const jobs = @import("relational_index_owner_job.zig");
    const physical = @import("relational_index_catalog.zig");
    const registry = @import("schema_registry.zig");
    const topology = @import("relational_integrity_topology.zig");
    const DocStore = @import("../docstore.zig").DocStore;
    var before = try schema_api.parseValidatedTableSchema(a, test_before);
    defer before.deinit(a);
    var after = try schema_api.parseValidatedTableSchema(a, test_after);
    defer after.deinit(a);
    var prior = try compiler.compileCatalog(a, before, 101, null);
    defer prior.deinit();
    var candidate = try Candidate.prepare(a, 101, test_before, test_after, prior.value, "email_idx");
    defer candidate.deinit();
    const before_schema = try schema_api.deriveRuntimeTableSchema(a, before);
    defer native_schema.freeSchema(a, before_schema);
    const after_schema = try schema_api.deriveRuntimeTableSchema(a, after);
    defer native_schema.freeSchema(a, after_schema);
    var before_registry = try registry.Registry.initCloned(a, std.testing.io, before_schema);
    defer before_registry.deinit();
    var before_view = before_registry.acquire().?;
    defer before_view.release();
    var after_registry = try registry.Registry.initCloned(a, std.testing.io, after_schema);
    defer after_registry.deinit();
    var after_view = after_registry.acquire().?;
    defer after_view.release();
    var declaration_arena = std.heap.ArenaAllocator.init(a);
    defer declaration_arena.deinit();
    const definitions = (try after.relationalIndexDefinitions(declaration_arena.allocator())).?;
    var private_addition = try physical.PrivateAddition.init(a, before_view, after_view, null, definitions, "email_idx");
    defer private_addition.deinit();
    var hash: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(prior.value, &hash, .{});
    const fence: topology.Fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 }, .catalog_digest = hash };
    const binding = try jobs.prepare(@splat(1), @splat(2), @splat(3), 0, fence, &candidate, &private_addition);
    var substituted_definitions = try declaration_arena.allocator().dupe(native_index.RelationalIndexDefinition, definitions);
    substituted_definitions[0].keys = &.{.{ .column = "id" }};
    var substituted = try physical.PrivateAddition.init(a, before_view, after_view, null, substituted_definitions, "email_idx");
    defer substituted.deinit();
    try std.testing.expectError(error.InvalidPrivateIndexOwnerJob, jobs.prepare(@splat(1), @splat(2), @splat(3), 0, fence, &candidate, &substituted));
    const encoded = try binding.encode();
    try std.testing.expectEqualSlices(u8, &encoded, &try (try jobs.Binding.decode(&encoded)).encode());
    var damaged = encoded;
    damaged[88] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexOwnerJob, jobs.Binding.decode(&damaged));
    var backend = Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "private-owner" });
    defer runtime.deinit();
    var store = try DocStore.openRuntime(a, &runtime);
    defer store.close();
    _ = try native_schema.saveSchema(&store, a, before_schema);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(catalog.key, prior.value);
        var namespace_bytes: [24]u8 = undefined;
        @import("doc_identity.zig").encodeNamespace(&namespace_bytes, fence.namespace);
        try txn.put(&@import("../internal_keys.zig").identity_namespace_key, &namespace_bytes);
        for ([_][]const u8{ "a", "b", "c" }, [_][]const u8{
            "{\"id\":1,\"email\":\"a@example.org\"}", "{\"id\":2,\"email\":\"b@example.org\"}", "{\"id\":3,\"email\":\"c@example.org\"}",
        }) |document, json| {
            const primary = try @import("../internal_keys.zig").relationalRowKeyAlloc(a, document);
            defer a.free(primary);
            const row = try @import("relational_store.zig").encodeValueForSchemaAlloc(a, json, before_schema);
            defer a.free(row);
            try txn.put(primary, row);
        }
        try txn.commit();
    }
    var manager = try @import("../transactions.zig").TxnManager.init(a, &store);
    defer manager.deinit();
    try manager.initTransaction(@splat(9), 1);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.PrivateIndexJobRequired, topology.stageBegin(&txn, fence));
        try std.testing.expect(try jobs.stageBegin(&txn, binding));
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expect(try jobs.load(&txn) == null);
        try std.testing.expect(try topology.current(&txn) == null);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try jobs.stageBegin(&txn, binding));
        try std.testing.expect(!try jobs.stageBegin(&txn, binding));
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.TransactionTopologyBusy, jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, jobs.requireReserved(&txn, binding));
        try std.testing.expectError(error.IntegrityTopologyBusy, topology.requireUnfenced(&txn));
        try std.testing.expectError(error.PrivateIndexPublicationRequired, topology.stageRelease(&txn, fence));
        try std.testing.expectError(error.PrivateIndexCancellationRequired, topology.stageCancel(&txn, fence));
        var foreign = binding;
        foreign.id[0] ^= 1;
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, jobs.stageBegin(&txn, foreign));
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, jobs.stageReservation(&txn, &manager, foreign, &candidate, &private_addition));
        try std.testing.expectEqual(jobs.Phase.fencing, (try jobs.load(&txn)).?.phase);
        try std.testing.expectEqualSlices(u8, prior.value, try txn.get(catalog.key));
        try std.testing.expectError(error.NotFound, txn.get(physical.head_key));
    }
    try manager.resolveIntents(@splat(9), .aborted, 2);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(jobs.key);
        try std.testing.expectError(error.PrivateIndexJobRequired, topology.current(&txn));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var foreign = binding;
        foreign.fence.attempt += 1;
        try txn.put(jobs.key, &try foreign.encode());
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, topology.current(&txn));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        // The integrity burn is attempted first; a later physical CAS failure
        // must roll it back with the entire caller-owned transaction.
        var empty = try physical.Prepared.init(a, before_view, null, &.{});
        defer empty.deinit();
        try txn.put(physical.head_key, &empty.head.encode());
        try std.testing.expectError(error.PreparedGenerationChanged, jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expect(try jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(jobs.Phase.fencing, (try jobs.load(&txn)).?.phase);
        try std.testing.expectEqualSlices(u8, prior.value, try txn.get(catalog.key));
        try std.testing.expectError(error.NotFound, txn.get(physical.head_key));
        try std.testing.expect(try jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
        try std.testing.expect(!try jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
        try std.testing.expect(!try jobs.stageBegin(&txn, binding));
        try txn.commit();
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(jobs.Phase.reserved, (try jobs.load(&txn)).?.phase);
        try std.testing.expectEqualSlices(u8, candidate.reservation.value, try txn.get(catalog.key));
        var active = (try physical.load(a, &txn)).?;
        defer active.deinit();
        try std.testing.expectEqual(@as(usize, 0), active.entries().len);
        try std.testing.expectEqual(before_view.version(), active.head.schema_version);
        var recovered = try jobs.reopenPhysical(a, &txn, before_view, after_view, "email_idx");
        defer recovered.deinit();
        try std.testing.expectEqual(binding.index.mapKey(), recovered.index.mapKey());
        try std.testing.expect(recovered.reservation.expected == null);
        try std.testing.expect(!try jobs.stageReservation(&txn, &manager, binding, &candidate, &recovered));
        try std.testing.expect(physical.Controller.isReservedMetadataKey(jobs.key));
    }
    try testPrivateBuildPages(a, &store, before_view, binding, &private_addition);
}

fn testPrivateBuildPages(a: Allocator, store: *@import("../docstore.zig").DocStore, before: @import("schema_registry.zig").SchemaView, binding: @import("relational_index_owner_job.zig").Binding, addition_plan: *const @import("relational_index_catalog.zig").PrivateAddition) !void {
    const jobs = @import("relational_index_jobs.zig");
    const physical = @import("relational_index_catalog.zig");
    const registry = @import("schema_registry.zig");
    const txns = @import("../transactions.zig");
    const Core = struct {
        alloc: Allocator,
        store: *@import("../docstore.zig").DocStore,
        relational_indexes: physical.Controller,
        identity_namespace: @import("doc_identity_namespace.zig").Namespace,
        before: registry.SchemaView,
        generation: u64 = 1,
        historical_reads: usize = 0,
        pub fn schemaNamespaceGeneration(self: *@This()) u64 {
            return self.generation;
        }
        pub fn acquireSchemaVersionView(self: *@This(), version: u32) !?registry.SchemaView {
            self.historical_reads += 1;
            return if (version == self.before.version()) self.before.clone() else null;
        }
        pub fn initTxnManager(self: *@This()) !txns.TxnManager {
            return txns.TxnManager.init(self.alloc, self.store);
        }
    };
    var core: Core = .{ .alloc = a, .store = store, .relational_indexes = try physical.Controller.init(a, std.testing.io, store, before), .identity_namespace = binding.fence.namespace, .before = before };
    defer core.relational_indexes.deinit();
    var manager = try core.initTxnManager();
    defer manager.deinit();
    const budget: jobs.Budget = .{ .records = 1, .time_ns = std.time.ns_per_s };
    {
        var foreign = binding;
        foreign.plan_digest[0] ^= 1;
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, jobs.Page.preparePrivate(a, std.testing.io, &core, foreign, addition_plan, budget));
    }
    {
        var page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)).?;
        defer page.deinit();
        try std.testing.expect(!core.relational_indexes.isCurrent(page.pinned));
        try std.testing.expectError(error.PrivateIndexOrderedApplyRequired, page.commit(&core));
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try page.stage(&core, &manager, &txn);
        try std.testing.expectError(error.RelationalIndexPageConsumed, page.stage(&core, &manager, &txn));
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(&jobs.progressKey(binding.index)));
        var cursor = try txn.openCursor();
        defer cursor.close();
        const prefix = try @import("relational_index_records.zig").forwardPrefix(binding.index);
        if (try cursor.seekAtOrAfter(&prefix)) |entry| try std.testing.expect(!std.mem.startsWith(u8, entry.key, &prefix));
    }
    {
        var page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)).?;
        defer page.deinit();
        core.generation += 1;
        defer core.generation -= 1;
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.PreparedGenerationChanged, page.stage(&core, &manager, &txn));
    }
    {
        var page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)).?;
        defer page.deinit();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const primary = try @import("../internal_keys.zig").relationalRowKeyAlloc(a, "a");
        defer a.free(primary);
        try txn.put(primary, "changed-after-prepare");
        try std.testing.expectError(error.PreparedGenerationChanged, page.stage(&core, &manager, &txn));
    }
    {
        var page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, .{ .records = 2, .time_ns = std.time.ns_per_s })).?;
        defer page.deinit();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const primary = try @import("../internal_keys.zig").relationalRowKeyAlloc(a, "b");
        defer a.free(primary);
        try txn.put(primary, "second-source-changed");
        // First-candidate writes must roll back when the second candidate
        // invalidates the frozen source. Progress must not skip that source.
        try std.testing.expectError(error.PreparedGenerationChanged, page.stage(&core, &manager, &txn));
    }
    var completed = false;
    var scan_work: usize = 0;
    {
        var page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)).?;
        defer page.deinit();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(@import("relational_integrity_topology.zig").fence_key);
        try std.testing.expectError(error.IntegrityTopologyFenceMissing, page.stage(&core, &manager, &txn));
    }
    {
        var page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)).?;
        defer page.deinit();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var foreign = binding.fence.namespace;
        foreign.range_id += 1;
        var namespace_bytes: [24]u8 = undefined;
        @import("doc_identity.zig").encodeNamespace(&namespace_bytes, foreign);
        try txn.put(&@import("../internal_keys.zig").identity_namespace_key, &namespace_bytes);
        try std.testing.expectError(error.IdentityNamespaceMismatch, page.stage(&core, &manager, &txn));
    }
    {
        var first_page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)).?;
        defer first_page.deinit();
        var stale_page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)).?;
        defer stale_page.deinit();
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try first_page.stage(&core, &manager, &txn);
            try txn.commit();
            scan_work += first_page.records_examined;
        }
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.PreparedGenerationChanged, stale_page.stage(&core, &manager, &txn));
    }
    for (0..128) |_| {
        var page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)) orelse {
            completed = true;
            break;
        };
        defer page.deinit();
        try std.testing.expect(page.records_examined <= budget.records);
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try page.stage(&core, &manager, &txn);
        try txn.put("\x00\x00__metadata__:private_page_test_outbox", "page-applied");
        try txn.commit();
        scan_work += page.records_examined;
    }
    try std.testing.expect(completed);
    try std.testing.expectEqual(@as(usize, 3), scan_work);
    try std.testing.expect(core.historical_reads != 0);
    var txn = try store.beginReadTxn();
    defer txn.abort();
    const progress = try jobs.Progress.decode(try txn.get(&jobs.progressKey(binding.index)));
    try std.testing.expectEqual(jobs.State.ready, progress.state);
    try std.testing.expectEqual(jobs.Phase.primary, progress.phase);
    try std.testing.expectEqual(@as(u64, 3), progress.rows_scanned);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const prefix = try @import("relational_index_records.zig").forwardPrefix(binding.index);
    var entry = try cursor.seekAtOrAfter(&prefix);
    var count: usize = 0;
    while (entry) |kv| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, kv.key, &prefix)) break;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), count);
    for ([_][]const u8{ "a", "b", "c" }, [_][]const u8{
        "{\"id\":1,\"email\":\"a@example.org\"}", "{\"id\":2,\"email\":\"b@example.org\"}", "{\"id\":3,\"email\":\"c@example.org\"}",
    }) |document, json| {
        const primary = try @import("../internal_keys.zig").relationalRowKeyAlloc(a, document);
        defer a.free(primary);
        const original = try @import("relational_store.zig").encodeValueForSchemaAlloc(a, json, before.tableSchema().*);
        defer a.free(original);
        try std.testing.expectEqualSlices(u8, original, try txn.get(primary));
    }
    var active = (try physical.load(a, &txn)).?;
    defer active.deinit();
    try std.testing.expectEqual(@as(usize, 0), active.entries().len);
    try std.testing.expectEqual(before.version(), active.head.schema_version);
    var published = core.relational_indexes.acquire().?;
    defer published.deinit();
    try std.testing.expectEqual(@as(usize, 0), published.plan.boundIndexes().len);
}

test "relational index candidate owner binding fences drains and reserves both catalogs atomically on memory and LSM" {
    try testOwnerJobAdmission(@import("../mem_backend.zig").Backend);
    try testOwnerJobAdmission(@import("../lsm_backend.zig").Backend);
}

test "relational index candidate preserves CHECK coverage and reserves without public schema change" {
    const a = std.testing.allocator;
    var schema = try schema_api.parseValidatedTableSchema(a, test_before);
    defer schema.deinit(a);
    var prior = try compiler.compileCatalog(a, schema, 101, null);
    defer prior.deinit();
    var candidate = try Candidate.prepare(a, 101, test_before, test_after, prior.value, "email_idx");
    defer candidate.deinit();
    try std.testing.expectEqual(@as(u32, 1), candidate.reservation.catalog.schema_version);
    try std.testing.expectEqual(@as(u32, 2), candidate.next.catalog.schema_version);
    try std.testing.expect(candidate.reservation.catalog.find(.unique, "email_idx") == null);
    try std.testing.expectEqualSlices(u8, &prior.catalog.checks_digest, &candidate.next.catalog.checks_digest);
    try std.testing.expectEqual(prior.catalog.next_generation + 1, candidate.reservation.catalog.next_generation);
    var installed = try catalog.prepareIndexPublication(a, prior.value, candidate.reservation.value, candidate.next.value, "email_idx");
    defer installed.deinit();
    try std.testing.expectEqualSlices(u8, candidate.next.value, installed.value);
}

test "relational index candidate rejects unrelated schema changes and incomplete index pairs" {
    const a = std.testing.allocator;
    try validateSchemaDelta(a, test_before, test_after, "email_idx");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const edits = [_]struct { from: []const u8, to: []const u8 }{
        .{ .from = "\"additionalProperties\":false", .to = "\"additionalProperties\":true" },
        .{ .from = "\"op\":\"gt\"", .to = "\"op\":\"gte\"" },
        .{ .from = "\"origin\":\"index\"", .to = "\"origin\":\"constraint\"" },
        .{ .from = "\"origin\":\"index\"", .to = "\"origin\":\"index\",\"primary\":true" },
        .{ .from = "\"origin\":\"index\"", .to = "\"origin\":\"index\",\"deferrable\":true" },
        .{ .from = "\"relational_indexes\"", .to = "\"other_indexes\"" },
        .{ .from = "\"unique_constraints\"", .to = "\"other_constraints\"" },
    };
    for (edits) |edit| {
        const changed = try std.mem.replaceOwned(u8, scratch, test_after, edit.from, edit.to);
        try std.testing.expectError(error.InvalidIndexPublicationCandidate, validateSchemaDelta(a, test_before, changed, "email_idx"));
    }
    // JSON object key order is irrelevant, unlike changes to row semantics.
    try std.testing.expect(equal(try std.json.parseFromSliceLeaky(Value, scratch, "{\"a\":1,\"b\":2}", .{}), try std.json.parseFromSliceLeaky(Value, scratch, "{\"b\":2,\"a\":1}", .{}), 0));
}

test "relational index candidate rejects stale predecessor and mismatched physical keys or predicates" {
    const a = std.testing.allocator;
    var schema = try schema_api.parseValidatedTableSchema(a, test_before);
    defer schema.deinit(a);
    var prior = try compiler.compileCatalog(a, schema, 101, null);
    defer prior.deinit();
    const wrong_key = try std.mem.replaceOwned(u8, a, test_after, "\"columns\":[\"email\"]", "\"columns\":[\"id\"]");
    defer a.free(wrong_key);
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, Candidate.prepare(a, 101, test_before, wrong_key, prior.value, "email_idx"));
    const wrong_where = try std.mem.replaceOwned(u8, a, test_after, "\"origin\":\"index\"", "\"origin\":\"index\",\"where\":[{\"column\":\"id\",\"op\":\"gt\",\"value\":0}]");
    defer a.free(wrong_where);
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, Candidate.prepare(a, 101, test_before, wrong_where, prior.value, "email_idx"));
    const stale = try std.mem.replaceOwned(u8, a, test_before, "\"version\":1", "\"version\":0");
    defer a.free(stale);
    var stale_schema = try schema_api.parseValidatedTableSchema(a, stale);
    defer stale_schema.deinit(a);
    var stale_catalog = try compiler.compileCatalog(a, stale_schema, 101, null);
    defer stale_catalog.deinit();
    try std.testing.expectError(error.IntegrityCatalogChanged, Candidate.prepare(a, 101, test_before, test_after, stale_catalog.value, "email_idx"));
    try std.testing.expectError(error.IntegrityCatalogIncarnationMismatch, Candidate.prepare(a, 102, test_before, test_after, prior.value, "email_idx"));
}

test "relational index candidate retry preserves the canceled attempt generation burn" {
    const a = std.testing.allocator;
    var schema = try schema_api.parseValidatedTableSchema(a, test_before);
    defer schema.deinit(a);
    var prior = try compiler.compileCatalog(a, schema, 101, null);
    defer prior.deinit();
    var first = try Candidate.prepare(a, 101, test_before, test_after, prior.value, "email_idx");
    defer first.deinit();
    // Cancellation keeps the reservation and the original public schema. A
    // retry must not resurrect the first private build's keys/unique claims.
    var retry = try Candidate.prepare(a, 101, test_before, test_after, first.reservation.value, "email_idx");
    defer retry.deinit();
    const first_binding = first.next.catalog.find(.unique, "email_idx").?;
    const retry_binding = retry.next.catalog.find(.unique, "email_idx").?;
    try std.testing.expectEqual(first_binding.allocation + 1, retry_binding.allocation);
    try std.testing.expect(!std.mem.eql(u8, &first_binding.generation, &retry_binding.generation));
    try std.testing.expectEqualSlices(u8, &prior.catalog.schema_digest, &retry.reservation.catalog.schema_digest);
    try std.testing.expectEqualSlices(u8, &prior.catalog.checks_digest, &retry.reservation.catalog.checks_digest);
    try std.testing.expect(retry.reservation.catalog.find(.unique, "email_idx") == null);
}
