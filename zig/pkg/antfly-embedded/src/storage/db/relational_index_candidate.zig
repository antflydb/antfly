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
const inputs_mod = @import("relational_index_inputs.zig");
pub const max_schema_bytes = inputs_mod.max_schema_bytes;

pub const Candidate = struct {
    next: catalog.Update,
    reservation: catalog.Update,
    definition_digest: [32]u8,
    inputs: inputs_mod.Prepared,

    pub fn deinit(self: *Candidate) void {
        self.inputs.deinit();
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
        var reservation = try catalog.prepareIndexReservation(alloc, predecessor, next.catalog, name);
        errdefer reservation.deinit();
        const inputs = try inputs_mod.Prepared.init(alloc, .{ .before_json = before_json, .after_json = after_json, .predecessor = predecessor, .name = name });
        return .{ .next = next, .reservation = reservation, .definition_digest = definition_digest, .inputs = inputs };
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
    const owned_inputs = try inputs_mod.decode(candidate.inputs.bytes);
    try std.testing.expectEqualStrings(test_before, owned_inputs.before_json);
    try std.testing.expectEqualStrings(test_after, owned_inputs.after_json);
    try std.testing.expectEqualSlices(u8, prior.value, owned_inputs.predecessor);
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

fn testWorkerInputAllocations(a: Allocator) !void {
    var prepared = try inputs_mod.Prepared.init(a, .{ .before_json = "{}", .after_json = "{}", .predecessor = "{}", .name = "idx" });
    defer prepared.deinit();
    const Reader = struct {
        bytes: []const u8,
        scratch: [256]u8 = undefined,
        failed: bool = false,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            if (self.failed) return error.BackendReadFailed;
            if (!std.mem.eql(u8, key, inputs_mod.key)) return error.NotFound;
            @memcpy(self.scratch[0..self.bytes.len], self.bytes);
            return self.scratch[0..self.bytes.len];
        }
    };
    var reader: Reader = .{ .bytes = prepared.bytes };
    var owned = try inputs_mod.Prepared.load(a, &reader, inputs_mod.digest(prepared.bytes));
    defer owned.deinit();
    @memset(&reader.scratch, 0);
    const view = try inputs_mod.decode(owned.bytes);
    try std.testing.expectEqualStrings("idx", view.name);
    reader.failed = true;
    try std.testing.expectError(error.BackendReadFailed, inputs_mod.Prepared.load(a, &reader, inputs_mod.digest(prepared.bytes)));
}

test "relational index candidate immutable worker inputs own borrowed bytes bound framing and clean allocation failures" {
    const a = std.testing.allocator;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, testWorkerInputAllocations, .{});
    var prepared = try inputs_mod.Prepared.init(a, .{ .before_json = "{}", .after_json = "{}", .predecessor = "{}", .name = "idx" });
    defer prepared.deinit();
    const corrupt = try a.dupe(u8, prepared.bytes);
    defer a.free(corrupt);
    corrupt[20] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexInputs, inputs_mod.decode(corrupt));
    var unbounded: [52]u8 = @splat(0);
    @memcpy(unbounded[0..4], "AIM1");
    std.mem.writeInt(u32, unbounded[4..8], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.InvalidPrivateIndexInputs, inputs_mod.decode(&unbounded));
    try std.testing.expectError(error.InvalidPrivateIndexInputs, inputs_mod.decode(prepared.bytes[0 .. prepared.bytes.len - 1]));
}

fn stageNativeReplayForTest(
    txn: *@import("../docstore.zig").DocStore.Txn,
    manager: *@import("../transactions.zig").TxnManager,
    binding: @import("relational_index_owner_job.zig").Binding,
    candidate: *const Candidate,
    physical: *const @import("relational_index_catalog.zig").PrivateAddition,
    effect: @import("relational_index_owner_job.zig").NativeEffect,
    position: @import("receipt_position.zig").Native,
) !void {
    const jobs = @import("relational_index_owner_job.zig");
    switch (effect) {
        .admitted => _ = try jobs.stageBeginWithReplay(txn, binding, &candidate.inputs, position),
        .fenced => _ = try jobs.stageNativeFencedWithReplay(txn, manager, binding, position),
        .reserved => _ = try jobs.stageReservationWithReplay(txn, manager, binding, candidate, physical, position),
    }
}

fn testRejectedNativeReplay(
    store: *@import("../docstore.zig").DocStore,
    manager: *@import("../transactions.zig").TxnManager,
    binding: @import("relational_index_owner_job.zig").Binding,
    candidate: *const Candidate,
    physical: *const @import("relational_index_catalog.zig").PrivateAddition,
    effect: @import("relational_index_owner_job.zig").NativeEffect,
    sequence: u64,
    duplicate: bool,
) !void {
    const Native = @import("receipt_position.zig").Native;
    var foreign = binding.fence.namespace;
    foreign.range_id += 1;
    const invalid = [_]struct { position: Native, err: anyerror }{
        .{ .position = .{ .namespace = binding.fence.namespace, .sequence = 0 }, .err = error.InvalidControlReceiptPosition },
        .{ .position = .{ .namespace = foreign, .sequence = sequence + 1 }, .err = error.IdentityNamespaceMismatch },
        .{ .position = .{ .namespace = binding.fence.namespace, .sequence = sequence + 2 }, .err = if (duplicate) error.InvalidControlReceiptPosition else error.OnlineSourceScopeChanged },
    };
    for (invalid) |case| {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(case.err, stageNativeReplayForTest(&txn, manager, binding, candidate, physical, effect, case.position));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var raft = binding;
        raft.authority = .raft;
        try std.testing.expectError(error.InvalidControlReceiptPosition, stageNativeReplayForTest(&txn, manager, raft, candidate, physical, effect, .{ .namespace = binding.fence.namespace, .sequence = sequence + 1 }));
    }
    var txn = try store.beginReadTxn();
    defer txn.abort();
    try std.testing.expectEqual(sequence, (try @import("../source_authority.zig").load(&txn)).?.sequence);
    const job = try @import("relational_index_owner_job.zig").load(&txn);
    if (sequence == 0) {
        try std.testing.expect(job == null);
        try std.testing.expect(try @import("relational_integrity_topology.zig").current(&txn) == null);
    } else {
        try std.testing.expectEqual(if (sequence < 3) @import("relational_index_owner_job.zig").Phase.fencing else .reserved, job.?.phase);
    }
    // A gap at reservation may stage both catalog burns before the clock
    // rejects it. Aborting must preserve the exact old catalog on both stores.
    try std.testing.expectEqualSlices(u8, if (sequence < 3) candidate.reservation.expected.? else candidate.reservation.value, try txn.get(catalog.key));
    if (sequence < 3) {
        if (binding.physical_before) |before| {
            try std.testing.expect((try @import("relational_index_catalog.zig").Head.decode(try txn.get(@import("relational_index_catalog.zig").head_key))).eql(before));
        } else try std.testing.expectError(error.NotFound, txn.get(@import("relational_index_catalog.zig").head_key));
    }
}

fn testOwnerJobAdmission(comptime Backend: type, comptime existing: bool, comptime replayed: bool) !void {
    const a = std.testing.allocator;
    const jobs = @import("relational_index_owner_job.zig");
    const physical = @import("relational_index_catalog.zig");
    const registry = @import("schema_registry.zig");
    const topology = @import("relational_integrity_topology.zig");
    const DocStore = @import("../docstore.zig").DocStore;
    const existing_declaration = "{\"name\":\"id_idx\",\"keys\":[{\"column\":\"id\"}]}";
    const before_json = if (existing) try std.mem.replaceOwned(u8, a, test_before, "\"version\":1", "\"version\":1,\"relational_indexes\":[" ++ existing_declaration ++ "]") else try a.dupe(u8, test_before);
    defer a.free(before_json);
    const after_json = if (existing) try std.mem.replaceOwned(u8, a, test_after, "\"relational_indexes\":[", "\"relational_indexes\":[" ++ existing_declaration ++ ",") else try a.dupe(u8, test_after);
    defer a.free(after_json);
    var before = try schema_api.parseValidatedTableSchema(a, before_json);
    defer before.deinit(a);
    var after = try schema_api.parseValidatedTableSchema(a, after_json);
    defer after.deinit(a);
    var prior = try compiler.compileCatalog(a, before, 101, null);
    defer prior.deinit();
    var candidate = try Candidate.prepare(a, 101, before_json, after_json, prior.value, "email_idx");
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
    var initial: ?physical.Prepared = if (existing) try physical.Prepared.init(a, before_view, null, (try before.relationalIndexDefinitions(declaration_arena.allocator())).?) else null;
    defer if (initial) |*value| value.deinit();
    var initial_loaded: ?physical.Loaded = if (initial) |value| try physical.decode(a, value.head, value.blob) else null;
    defer if (initial_loaded) |*value| value.deinit();
    const definitions = (try after.relationalIndexDefinitions(declaration_arena.allocator())).?;
    var private_addition = try physical.PrivateAddition.init(a, before_view, after_view, if (initial_loaded) |*value| value else null, definitions, "email_idx");
    defer private_addition.deinit();
    var hash: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(prior.value, &hash, .{});
    const fence: topology.Fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 }, .catalog_digest = hash };
    const binding = try jobs.prepare(@splat(1), @splat(2), @splat(3), 0, fence, .native, &candidate, &private_addition);
    const substituted_definitions = try declaration_arena.allocator().dupe(native_index.RelationalIndexDefinition, definitions);
    for (substituted_definitions) |*definition| if (std.mem.eql(u8, definition.name, "email_idx")) {
        definition.keys = &.{.{ .column = "id" }};
    };
    var substituted = try physical.PrivateAddition.init(a, before_view, after_view, if (initial_loaded) |*value| value else null, substituted_definitions, "email_idx");
    defer substituted.deinit();
    try std.testing.expectError(error.InvalidPrivateIndexOwnerJob, jobs.prepare(@splat(1), @splat(2), @splat(3), 0, fence, .native, &candidate, &substituted));
    const encoded = try binding.encode();
    try std.testing.expectEqualSlices(u8, &encoded, &try (try jobs.Binding.decode(&encoded)).encode());
    var damaged = encoded;
    damaged[88] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexOwnerJob, jobs.Binding.decode(&damaged));
    // One LSM profile uses an actual WAL-backed root so claim retirement can
    // reopen the complete owner between pages rather than simulate a restart
    // by releasing a transaction against an unchanged in-memory backend.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var root_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const restart_path: ?[]const u8 = if (Backend == @import("../lsm_backend.zig").Backend and existing and !replayed)
        try std.fmt.bufPrint(&root_buffer, ".zig-cache/tmp/{s}", .{tmp.sub_path})
    else
        null;
    var storage_live = true;
    var backend = if (comptime Backend == @import("../lsm_backend.zig").Backend) blk: {
        break :blk if (restart_path) |path| try Backend.open(a, path, .{ .flush_threshold = 2, .wal_enabled = true }) else Backend.init(a, .{});
    } else Backend.init(a, .{});
    defer if (storage_live) backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "private-owner" });
    defer if (storage_live) runtime.deinit();
    var store = try DocStore.openRuntime(a, &runtime);
    defer if (storage_live) store.close();
    _ = try native_schema.saveSchema(&store, a, before_schema);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(catalog.key, prior.value);
        if (initial) |*value| _ = try value.stage(&txn);
        try @import("doc_identity.zig").writeNamespaceTxn(&txn, fence.namespace);
        for ([_][]const u8{ "a", "b", "c" }, [_][]const u8{
            "{\"id\":1,\"email\":\"a@example.org\"}", "{\"id\":2,\"email\":\"b@example.org\"}", "{\"id\":3,\"email\":\"c@example.org\"}",
        }) |document, json| {
            const primary = try @import("../internal_keys.zig").relationalRowKeyAlloc(a, document);
            defer a.free(primary);
            const row = try @import("relational_store.zig").encodeValueForSchemaAlloc(a, json, before_schema);
            defer a.free(row);
            try txn.put(primary, row);
            _ = try @import("doc_identity.zig").ensureOrdinalForNamespaceTxn(a, &txn, fence.namespace, 1, document);
        }
        try txn.commit();
    }
    const authority = @import("../source_authority.zig");
    const namespace_bytes = @import("online_source_contract.zig").namespaceBytes(fence.namespace);
    const Native = @import("receipt_position.zig").Native;
    const admission_replay: ?Native = if (replayed) .{ .namespace = fence.namespace, .sequence = 1 } else null;
    const fenced_replay: ?Native = if (replayed) .{ .namespace = fence.namespace, .sequence = 2 } else null;
    const reserved_replay: ?Native = if (replayed) .{ .namespace = fence.namespace, .sequence = 3 } else null;
    {
        // Provision authority after loading rows, never by mixing a control
        // mutation with raw primary writes in a captured source transaction.
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try authority.bind(&txn, .native, namespace_bytes);
        try txn.commit();
    }
    var manager = try @import("../transactions.zig").TxnManager.init(a, &store);
    defer if (storage_live) manager.deinit();
    try manager.initTransaction(@splat(9), 1);
    {
        // An otherwise well-formed manifest without its atomically committed
        // binding is not recoverable and cannot be adopted by a new admission.
        var txn = try store.runtime_store.beginWrite();
        errdefer txn.abort();
        try txn.put(inputs_mod.key, candidate.inputs.bytes);
        try txn.commit();
    }
    try std.testing.expectError(error.InvalidPrivateIndexInputs, physical.Controller.loadSnapshot(a, &store, before_view));
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.InvalidPrivateIndexInputs, jobs.stageBegin(&txn, binding, &candidate.inputs));
        try std.testing.expect(try jobs.load(&txn) == null);
        try std.testing.expect(try topology.current(&txn) == null);
    }
    {
        var txn = try store.runtime_store.beginWrite();
        errdefer txn.abort();
        try txn.delete(inputs_mod.key);
        try txn.commit();
    }
    {
        var view = try inputs_mod.decode(candidate.inputs.bytes);
        view.name = "substituted_idx";
        var substituted_inputs = try inputs_mod.Prepared.init(a, view);
        defer substituted_inputs.deinit();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.InvalidPrivateIndexInputs, jobs.stageBegin(&txn, binding, &substituted_inputs));
        try std.testing.expect(try jobs.load(&txn) == null);
        try std.testing.expectError(error.NotFound, txn.get(inputs_mod.key));
    }
    if (replayed) try testRejectedNativeReplay(&store, &manager, binding, &candidate, &private_addition, .admitted, 0, false);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.PrivateIndexJobRequired, topology.stageBegin(&txn, fence));
        try std.testing.expect(try jobs.stageBeginWithReplay(&txn, binding, &candidate.inputs, admission_replay));
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expect(try jobs.load(&txn) == null);
        try std.testing.expect(try topology.current(&txn) == null);
        try std.testing.expectEqual(@as(u64, 0), (try authority.load(&txn)).?.sequence);
        try std.testing.expectError(error.NotFound, txn.get(&jobs.nativeEffectKey(.admitted)));
        try std.testing.expectError(error.NotFound, txn.get(inputs_mod.key));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try jobs.stageBeginWithReplay(&txn, binding, &candidate.inputs, admission_replay));
        try std.testing.expect(!try jobs.stageBeginWithReplay(&txn, binding, &candidate.inputs, admission_replay));
        try std.testing.expect(!try jobs.stageBegin(&txn, binding, &candidate.inputs));
        try std.testing.expectEqual(@as(u64, 1), (try authority.load(&txn)).?.sequence);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.TransactionTopologyBusy, jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
        try std.testing.expectError(error.TransactionTopologyBusy, jobs.stageNativeFenced(&txn, &manager, binding));
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, jobs.nativeReceipt(&txn, binding, .fenced));
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, jobs.requireReserved(&txn, binding));
        try std.testing.expectError(error.IntegrityTopologyBusy, topology.requireUnfenced(&txn));
        try std.testing.expectError(error.PrivateIndexPublicationRequired, topology.stageRelease(&txn, fence));
        try std.testing.expectError(error.PrivateIndexCancellationRequired, topology.stageCancel(&txn, fence));
        var foreign = binding;
        foreign.id[0] ^= 1;
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, jobs.stageBegin(&txn, foreign, &candidate.inputs));
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, jobs.stageReservation(&txn, &manager, foreign, &candidate, &private_addition));
        try std.testing.expectEqual(jobs.Phase.fencing, (try jobs.load(&txn)).?.phase);
        try std.testing.expectEqualSlices(u8, prior.value, try txn.get(catalog.key));
        if (initial) |value| {
            try std.testing.expect((try physical.Head.decode(try txn.get(physical.head_key))).eql(value.head));
        } else try std.testing.expectError(error.NotFound, txn.get(physical.head_key));
    }
    try manager.resolveIntents(@splat(9), .aborted, 2);
    if (replayed) try testRejectedNativeReplay(&store, &manager, binding, &candidate, &private_addition, .fenced, 1, false);
    try testOwnerRecovery(a, &store, before_view, binding, false);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
        _ = try jobs.stageNativeFencedWithReplay(&txn, &manager, binding, fenced_replay);
        try std.testing.expectEqual(@as(u64, 2), (try authority.load(&txn)).?.sequence);
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(u64, 1), (try authority.load(&txn)).?.sequence);
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, jobs.nativeReceipt(&txn, binding, .fenced));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const receipt = try jobs.stageNativeFencedWithReplay(&txn, &manager, binding, fenced_replay);
        try std.testing.expectEqual(@as(u64, 2), receipt.position.native.sequence);
        try std.testing.expectEqualDeep(receipt, try jobs.stageNativeFencedWithReplay(&txn, &manager, binding, fenced_replay));
        try std.testing.expectEqualDeep(receipt, try jobs.stageNativeFenced(&txn, &manager, binding));
        try txn.commit();
    }
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
    if (replayed) try testRejectedNativeReplay(&store, &manager, binding, &candidate, &private_addition, .reserved, 2, false);
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
        try std.testing.expect(try jobs.stageReservationWithReplay(&txn, &manager, binding, &candidate, &private_addition, reserved_replay));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(jobs.Phase.fencing, (try jobs.load(&txn)).?.phase);
        try std.testing.expectEqualSlices(u8, prior.value, try txn.get(catalog.key));
        if (initial) |value| {
            try std.testing.expect((try physical.Head.decode(try txn.get(physical.head_key))).eql(value.head));
        } else try std.testing.expectError(error.NotFound, txn.get(physical.head_key));
        try std.testing.expect(try jobs.stageReservationWithReplay(&txn, &manager, binding, &candidate, &private_addition, reserved_replay));
        try std.testing.expect(!try jobs.stageReservationWithReplay(&txn, &manager, binding, &candidate, &private_addition, reserved_replay));
        try std.testing.expect(!try jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
        try std.testing.expect(!try jobs.stageBegin(&txn, binding, &candidate.inputs));
        try std.testing.expectEqual(@as(u64, 3), (try authority.load(&txn)).?.sequence);
        try txn.commit();
    }
    if (replayed) {
        inline for (std.enums.values(jobs.NativeEffect)) |effect| try testRejectedNativeReplay(&store, &manager, binding, &candidate, &private_addition, effect, 3, true);
        var txn = try store.beginReadTxn();
        defer txn.abort();
        // Admission/fence retries retain their original cuts after reservation
        // has advanced the clock. They must not require the clock to move back.
        try std.testing.expect(!try jobs.stageBeginWithReplay(&txn, binding, &candidate.inputs, admission_replay));
        try std.testing.expectEqualDeep(fenced_replay.?, (try jobs.stageNativeFencedWithReplay(&txn, &manager, binding, fenced_replay)).position.native);
        try std.testing.expect(!try jobs.stageReservationWithReplay(&txn, &manager, binding, &candidate, &private_addition, reserved_replay));
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(jobs.Phase.reserved, (try jobs.load(&txn)).?.phase);
        try std.testing.expectEqualSlices(u8, candidate.reservation.value, try txn.get(catalog.key));
        var active = (try physical.load(a, &txn)).?;
        defer active.deinit();
        try std.testing.expectEqual(@as(usize, @intFromBool(existing)), active.entries().len);
        try std.testing.expectEqual(before_view.version(), active.head.schema_version);
        var recovered = try jobs.reopenPhysical(a, &txn, before_view, after_view, "email_idx");
        defer recovered.deinit();
        try std.testing.expectEqual(binding.index.mapKey(), recovered.index.mapKey());
        try std.testing.expectEqual(existing, recovered.reservation.expected != null);
        try std.testing.expect(!try jobs.stageReservation(&txn, &manager, binding, &candidate, &recovered));
        try std.testing.expect(physical.Controller.isReservedMetadataKey(jobs.key));
        try std.testing.expect(physical.Controller.isReservedMetadataKey(inputs_mod.key));
        for (std.enums.values(jobs.NativeEffect)) |effect| try std.testing.expect(physical.Controller.isReservedMetadataKey(&jobs.nativeEffectKey(effect)));
        const fenced = try jobs.nativeReceipt(&txn, binding, .fenced);
        const reserved = try jobs.nativeReceipt(&txn, binding, .reserved);
        try std.testing.expectEqual(@as(u64, 2), fenced.position.native.sequence);
        try std.testing.expectEqual(@as(u64, 3), reserved.position.native.sequence);
        // Feed actual locally derived proofs through the shared metadata
        // progress reducer. This is not server/RPC or root-signature evidence.
        const progress = @import("relational_index_publication_progress.zig");
        const head: progress.Head = .{ .plan_id = binding.id, .plan_digest = binding.plan_digest, .candidate_digest = binding.integrity_candidate, .reservation_digest = binding.integrity_reserved, .table_id = binding.fence.namespace.table_id, .owner_count = 1 };
        const fenced_update = try progress.acknowledge(head, .{ .owner = fenced.owner }, head.revision, fenced);
        const reserved_update = try progress.acknowledge(fenced_update.head, fenced_update.owner, fenced_update.head.revision, reserved);
        try std.testing.expectEqual(progress.Phase.building, reserved_update.head.phase);
    }
    try testOwnerRecovery(a, &store, before_view, binding, true);
    try testPrivateBuildPages(a, &store, before_view, binding, &private_addition);
    try testOwnerRecovery(a, &store, before_view, binding, true);
    try testPrivateClaimInventory(a, &store, &manager, binding, .{ .backend = &backend, .runtime = &runtime, .store = &store, .manager = &manager, .path = restart_path, .live = &storage_live });
}

fn reopenPrivateClaimTestStore(a: Allocator, state: anytype) !void {
    if (comptime @TypeOf(state.backend.*) == @import("../lsm_backend.zig").Backend) {
        const path = state.path orelse return;
        state.manager.deinit();
        state.store.close();
        state.runtime.deinit();
        state.backend.close();
        state.live.* = false;
        state.backend.* = try @import("../lsm_backend.zig").Backend.open(a, path, .{ .flush_threshold = 2, .wal_enabled = true });
        errdefer state.backend.close();
        state.runtime.* = try state.backend.runtimeStore(a, .{ .name = "private-owner" });
        errdefer state.runtime.deinit();
        state.store.* = try @import("../docstore.zig").DocStore.openRuntime(a, state.runtime);
        errdefer state.store.close();
        state.manager.* = try @import("../transactions.zig").TxnManager.init(a, state.store);
        state.live.* = true;
    }
}

fn testPrivateClaimInventory(a: Allocator, store: *@import("../docstore.zig").DocStore, manager: *@import("../transactions.zig").TxnManager, binding: @import("relational_index_owner_job.zig").Binding, restart: anytype) !void {
    const claims = @import("relational_index_private_claims.zig");
    const integrity = @import("relational_integrity_contract.zig");
    const jobs = @import("relational_index_owner_job.zig");
    const first: integrity.Claim = .{ .tuple = "first", .parent_table = "articles", .parent_key = "a", .schema_version = binding.physical_candidate.schema_version };
    const second: integrity.Claim = .{ .tuple = "second", .parent_table = "articles", .parent_key = "b", .schema_version = binding.physical_candidate.schema_version };
    const retained = @import("../retained_effects.zig");
    if (restart.path != null) {
        // The cold profile retains ordinary public integrity effects. Private
        // batches and derived GC must not enter this tail or advance it again.
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try retained.admit(&txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), 42, @splat(7), retained.default_limit);
        try txn.commit();
    }
    const foreign_address = try integrity.Address.init(@splat(238), first.tuple);
    const foreign_value = try first.encode(a, foreign_address);
    defer a.free(foreign_value);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.put(&foreign_address.claimKey(), foreign_value);
        try std.testing.expectError(error.RetainedEffectsMixedControl, @import("../source_authority.zig").advance(&txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), null));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(&foreign_address.claimKey(), foreign_value);
        try txn.commit();
    }
    const retained_start = blk: {
        var read = try store.beginReadTxn();
        defer read.abort();
        break :blk if (try retained.load(&read)) |value| value.latest else @as(?u64, null);
    };
    if (restart.path != null) try std.testing.expectEqual(@as(?u64, 1), retained_start);
    var prepared = blk: {
        var read = try store.beginReadTxn();
        defer read.abort();
        break :blk try claims.Prepared.init(a, &read, binding, &.{ first, second, first });
    };
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 2), prepared.entries.len);
    {
        const native = @import("relational_index_owner_control.zig");
        var oversized = std.Io.Writer.Allocating.init(a);
        defer oversized.deinit();
        try oversized.writer.writeAll("{\"claims\":[");
        for (0..claims.max_claims) |ordinal| {
            if (ordinal != 0) try oversized.writer.writeAll(",");
            try std.json.Stringify.value(first, .{}, &oversized.writer);
        }
        // A malformed 129th item must never be parsed; otherwise this reports
        // UnexpectedToken instead of the bounded-container LengthMismatch.
        try oversized.writer.writeAll(",null]}");
        try std.testing.expectError(error.LengthMismatch, std.json.parseFromSlice(native.ClaimCommand, a, oversized.written(), .{}));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const result = try prepared.stageOrdered(&txn, null);
        try std.testing.expect(!result.duplicate);
        try std.testing.expectEqual(@as(usize, 2), result.claims_added);
        try std.testing.expectError(error.RetainedEffectsMixedControl, txn.put(&prepared.entries[0].address.claimKey(), prepared.entries[0].value));
        try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.commit());
    }
    const original_cut = blk: {
        var read = try store.beginReadTxn();
        defer read.abort();
        const selected = try claims.requireRegistered(&read, binding);
        try std.testing.expectError(error.NotFound, read.get(&claims.inventoryKey(prepared.entries[0].address)));
        try std.testing.expectError(error.NotFound, read.get(&(try integrity.Address.init(selected.generation, first.tuple)).claimKey()));
        break :blk (try @import("../source_authority.zig").load(&read)).?.sequence + 1;
    };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const result = try prepared.stageOrdered(&txn, null);
        try std.testing.expect(!result.duplicate);
        try std.testing.expectEqual(@as(usize, 2), result.claims_added);
        try std.testing.expectEqual(original_cut, result.position.sequence);
        try txn.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(original_cut, (try @import("../source_authority.zig").load(&read)).?.sequence);
        try std.testing.expectEqual(retained_start, if (try retained.load(&read)) |value| value.latest else @as(?u64, null));
    }
    const allocation = blk: {
        var read = try store.beginReadTxn();
        defer read.abort();
        break :blk try claims.requireRegistered(&read, binding);
    };
    const address = try integrity.Address.init(allocation.generation, first.tuple);
    const inventory_key = claims.inventoryKey(address);
    try std.testing.expect(@import("relational_index_catalog.zig").Controller.isReservedMetadataKey(&inventory_key));
    try std.testing.expect(@import("relational_index_catalog.zig").Controller.isReservedMetadataKey(&claims.registrationKey(allocation.generation)));
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var reordered = try claims.Prepared.init(a, &txn, binding, &.{ second, first, second });
        defer reordered.deinit();
        for (prepared.entries, reordered.entries) |left, right| try std.testing.expectEqualSlices(u8, &left.address.claimKey(), &right.address.claimKey());
        const result = try reordered.stageOrdered(&txn, null);
        try std.testing.expect(result.duplicate);
        try std.testing.expectEqual(original_cut, result.position.sequence);
        try std.testing.expectEqual(original_cut, (try @import("../source_authority.zig").load(&txn)).?.sequence);
        try std.testing.expect((try reordered.stageOrdered(&txn, result.position)).duplicate);
        var wrong_cut = result.position;
        wrong_cut.sequence += 1;
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, reordered.stageOrdered(&txn, wrong_cut));
        const raw = try a.dupe(u8, try txn.get(&inventory_key));
        defer a.free(raw);
        raw[12] ^= 1;
        try txn.put(&inventory_key, raw);
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, reordered.stageOrdered(&txn, null));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var duplicate = try claims.Prepared.init(a, &txn, binding, &.{first});
        defer duplicate.deinit();
        const subset = try duplicate.stageOrdered(&txn, null);
        try std.testing.expect(subset.duplicate);
        try std.testing.expectEqual(original_cut, subset.position.sequence);
        try std.testing.expectEqual(@as(usize, 0), subset.claims_added);
        const transfer = @import("relational_integrity.zig");
        const transfer_view = try transfer.TransferView.init(&txn);
        try transfer.validateTransferredCompanionsWithView(&txn, &address.claimKey(), try txn.get(&address.claimKey()), transfer_view);
        try transfer.validateTransferredCompanions(&txn, &address.claimKey(), try txn.get(&address.claimKey()));
        var conflicting = first;
        conflicting.parent_key = "different-owner";
        try std.testing.expectError(error.UniqueConstraintViolation, claims.Prepared.init(a, &txn, binding, &.{conflicting}));
        try txn.delete(&inventory_key);
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, claims.Prepared.init(a, &txn, binding, &.{first}));
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, transfer.validateTransferredCompanionsWithView(&txn, &address.claimKey(), try txn.get(&address.claimKey()), transfer_view));
        try txn.put(&inventory_key, &@as([32]u8, @splat(0)));
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, claims.Prepared.init(a, &txn, binding, &.{first}));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(&claims.registrationKey(allocation.generation));
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, testOwnerRecoveryAllocations(a, &txn));
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const Harness = struct {
            fn run(alloc: Allocator, txn: *@import("../docstore.zig").DocStore.Txn, current: jobs.Binding, claim: integrity.Claim) !void {
                var batch = try claims.Prepared.init(alloc, txn, current, &.{claim});
                defer batch.deinit();
            }
            fn ownedControl(alloc: Allocator, txn: *@import("../docstore.zig").DocStore.Txn, current: jobs.Binding, claim: integrity.Claim) !void {
                const control = @import("relational_index_owner_control.zig");
                var reserved_binding = current;
                reserved_binding.phase = .reserved;
                var prepared_control = try control.PreparedClaims.init(alloc, txn, .{ .binding = reserved_binding, .claims = &.{claim} });
                defer prepared_control.deinit();
                try prepared_control.preparePayload();
                const bytes = try prepared_control.finalizePayload(.{ .namespace = current.fence.namespace, .sequence = 4 });
                var wire = try std.json.parseFromSlice(struct { schema_version: u32, request: struct {}, index_owner_control: control.Frame }, alloc, bytes, .{});
                defer wire.deinit();
                try wire.value.index_owner_control.validate();
                try std.testing.expectEqualSlices(u8, claim.tuple, wire.value.index_owner_control.effect.claims.claims[0].tuple);
                const ExcludedOwner = struct {
                    lower: [33]u8,
                    pub fn byteRange(self: *const @This()) @import("types.zig").ByteRange {
                        return .{ .start = &self.lower, .end = "" };
                    }
                };
                var excluded: ExcludedOwner = undefined;
                @memcpy(excluded.lower[0..32], &prepared_control.batch.entries[0].address.routing);
                excluded.lower[32] = 0;
                try std.testing.expectError(error.KeyOutOfRange, prepared_control.stage(&excluded, txn, null));
            }
        };
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Harness.run, .{ &read, binding, first });
        var binary = first;
        binary.tuple = "\x00\xff\x80binary-tuple";
        try Harness.ownedControl(a, &read, binding, binary);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Harness.ownedControl, .{ &read, binding, binary });
    }
    // More than one full GC page, with request buffers overwritten before
    // apply, proves both bounded continuation and prepared-byte ownership.
    for (0..2) |batch_number| {
        var buffers: [claims.max_claims][32]u8 = undefined;
        var bulk: [claims.max_claims]integrity.Claim = undefined;
        for (&buffers, &bulk, 0..) |*buffer, *claim, ordinal| {
            const tuple = try std.fmt.bufPrint(buffer, "bulk-{d}-{d}", .{ batch_number, ordinal });
            claim.* = .{ .tuple = tuple, .parent_table = "articles", .parent_key = tuple, .schema_version = binding.physical_candidate.schema_version };
        }
        var batch = blk: {
            var read = try store.beginReadTxn();
            defer read.abort();
            break :blk try claims.Prepared.init(a, &read, binding, &bulk);
        };
        defer batch.deinit();
        for (&buffers) |*buffer| @memset(buffer, 'x');
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(@as(usize, claims.max_claims), (try batch.stageOrdered(&txn, null)).claims_added);
        try txn.commit();
    }
    // Repartitioning existing work must not create a combinatorial receipt
    // ledger. Distinct mixed-age subsets resolve by immutable member cuts.
    for (0..claims.max_claims) |ordinal| {
        var buffer: [32]u8 = undefined;
        const tuple = try std.fmt.bufPrint(&buffer, "bulk-{d}-{d}", .{ ordinal % 2, ordinal });
        const bulk_claim: integrity.Claim = .{ .tuple = tuple, .parent_table = "articles", .parent_key = tuple, .schema_version = binding.physical_candidate.schema_version };
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var regrouped = try claims.Prepared.init(a, &txn, binding, &.{ bulk_claim, first });
        defer regrouped.deinit();
        const expected_cut = original_cut + 1 + ordinal % 2;
        const result = try regrouped.stageOrdered(&txn, .{ .namespace = binding.fence.namespace, .sequence = expected_cut });
        try std.testing.expect(result.duplicate);
        try std.testing.expectEqual(expected_cut, result.position.sequence);
        try std.testing.expectEqual(@as(usize, 0), result.claims_added);
        try std.testing.expectEqual(original_cut + 2, (try @import("../source_authority.zig").load(&txn)).?.sequence);
        try txn.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        const selected = inventory_key[0 .. claims.prefix.len + 16];
        var count: usize = 0;
        var next = try cursor.seekAtOrAfter(selected);
        while (next) |entry| : (next = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, selected)) break;
            try std.testing.expectEqual(@as(usize, claims.inventory_value_len), entry.value.len);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 2 + 2 * claims.max_claims), count);
    }
    {
        const large = try a.alloc(u8, claims.max_bytes);
        defer a.free(large);
        @memset(large, 'x');
        for (0..2) |ordinal| {
            var buffer: [32]u8 = undefined;
            var txn = try store.beginWriteTxn();
            var txn_live = true;
            defer if (txn_live) txn.abort();
            const large_claim: integrity.Claim = .{ .tuple = try std.fmt.bufPrint(&buffer, "large-{d}", .{ordinal}), .parent_table = "articles", .parent_key = large[0 .. 600 * 1024], .schema_version = binding.physical_candidate.schema_version };
            var batch = try claims.Prepared.init(a, &txn, binding, &.{large_claim});
            defer batch.deinit();
            const extra: integrity.Claim = .{ .tuple = "overlapping-new-member", .parent_table = "articles", .parent_key = "extra", .schema_version = binding.physical_candidate.schema_version };
            // Both preparations observe the large member as absent. The
            // second apply must accept its identical concurrent creation,
            // retain that earlier cut and stamp only its own new member.
            var overlapping: ?claims.Prepared = if (ordinal == 0) try claims.Prepared.init(a, &txn, binding, &.{ large_claim, second, extra }) else null;
            defer if (overlapping) |*value| value.deinit();
            const original = try batch.stageOrdered(&txn, null);
            try std.testing.expectEqual(@as(usize, 1), original.claims_added);
            try txn.commit();
            txn_live = false;
            if (overlapping) |*value| {
                var next = try store.beginWriteTxn();
                var next_live = true;
                defer if (next_live) next.abort();
                const result = try value.stageOrdered(&next, null);
                try std.testing.expect(!result.duplicate);
                try std.testing.expectEqual(@as(usize, 1), result.claims_added);
                try std.testing.expectEqual(original.position.sequence + 1, result.position.sequence);
                try next.commit();
                next_live = false;
                var probe = try store.beginReadTxn();
                defer probe.abort();
                const earlier = try batch.stageOrdered(&probe, original.position);
                const later = try value.stageOrdered(&probe, result.position);
                try std.testing.expect(earlier.duplicate and later.duplicate);
                try std.testing.expectEqual(original.position, earlier.position);
                try std.testing.expectEqual(result.position, later.position);
                try std.testing.expectEqual(result.position.sequence, (try @import("../source_authority.zig").load(&probe)).?.sequence);
            }
        }
        var read = try store.beginReadTxn();
        defer read.abort();
        var oversized = first;
        oversized.parent_key = large[0 .. claims.max_bytes - claims.claim_entry_overhead];
        try std.testing.expectError(error.TransactionTooLarge, claims.Prepared.init(a, &read, binding, &.{oversized}));
        var reserved_binding = binding;
        reserved_binding.phase = .reserved;
        try std.testing.expectError(error.TransactionTooLarge, @import("relational_index_owner_control.zig").ClaimCommand.validate(.{ .binding = reserved_binding, .claims = &.{oversized} }));
    }
    var third = first;
    third.tuple = "third";
    var late = blk: {
        var read = try store.beginReadTxn();
        defer read.abort();
        break :blk try claims.Prepared.init(a, &read, binding, &.{third});
    };
    defer late.deinit();
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const clock = (try @import("../source_authority.zig").load(&txn)).?.sequence;
        try std.testing.expectError(error.OnlineSourceScopeChanged, late.stageOrdered(&txn, .{ .namespace = binding.fence.namespace, .sequence = clock + 2 }));
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(&claims.inventoryKey(late.entries[0].address)));
        // A late retry of the original batch still recovers its original cut,
        // not the clock advanced by two later bulk claim effects.
        const retry = try prepared.stageOrdered(&txn, null);
        try std.testing.expect(retry.duplicate);
        try std.testing.expectEqual(original_cut, retry.position.sequence);
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(usize, 1), (try late.stageOrdered(&txn, null)).claims_added);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try jobs.stageCancellationIntentWithReplay(&txn, @import("relational_index_cancellation_intent.zig").Intent.fromBinding(binding), null);
        _ = try jobs.stageCancellationCleanupWithReplay(&txn, manager, binding, null);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.PrivateIndexPublicationCanceled, late.stageOrdered(&txn, null));
        try std.testing.expectError(error.PrivateIndexPublicationCanceled, prepared.stageOrdered(&txn, null));
    }
    try reopenPrivateClaimTestStore(a, restart);
    const cleanup_clock = blk: {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.PrivateIndexCancellationPending, claims.requireRetired(&read, binding));
        break :blk (try @import("../source_authority.zig").load(&read)).?.sequence;
    };
    var collected: usize = 0;
    var gc_pages: usize = 0;
    while (true) {
        var page = blk: {
            var read = try store.beginReadTxn();
            defer read.abort();
            break :blk (try claims.GcPage.prepare(a, std.testing.io, &read, binding)) orelse break;
        };
        defer page.deinit();
        try std.testing.expect(page.entries.len > 0 and page.entries.len <= 256);
        var page_bytes: usize = 0;
        for (page.entries) |entry| page_bytes += entry.value.len + claims.inventoryKey(entry.address).len + entry.address.claimKey().len + claims.inventory_value_len;
        try std.testing.expect(page_bytes <= claims.max_bytes);
        gc_pages += 1;
        try std.testing.expect(gc_pages <= 5 + 2 * claims.max_claims);
        if (page.entries.len != 0) {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            const selected = page.entries[0].address;
            const reference: integrity.Reference = .{ .child_table = "child", .child_key = "c", .constraint_name = "unexpected_fk", .constraint_generation = @splat(99) };
            const reference_value = try reference.encode(a, selected);
            defer a.free(reference_value);
            // Deliberately bypass capture to inject an on-disk corruption.
            // The normal DocStore path rejects unpublished FK companions.
            try txn.write.?.put(&try reference.key(selected), reference_value);
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, page.stage(&txn));
            const transfer = @import("relational_integrity.zig");
            const view = try transfer.TransferView.init(&txn);
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, transfer.validateTransferredCompanionsWithView(&txn, &try reference.key(selected), reference_value, view));
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, view.validateCompanion(&txn, .{ .address = selected, .kind = .reference }, @splat(0)));
        }
        if (page.entries.len != 0) {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            const selected = page.entries[0].address;
            const reference: integrity.Reference = .{ .child_table = "child", .child_key = "c", .constraint_name = "unexpected_fk", .constraint_generation = @splat(99) };
            const value = try reference.encode(a, selected);
            defer a.free(value);
            try page.stage(&txn);
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, txn.put(&try reference.key(selected), value));
            try std.testing.expectError(error.RetainedEffectsTransactionFailed, txn.delete(&selected.claimKey()));
        }
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            const selected_key = claims.inventoryKey(page.entries[0].address);
            const damaged = try a.dupe(u8, try txn.get(&selected_key));
            defer a.free(damaged);
            damaged[damaged.len - 1] ^= 1;
            try txn.put(&selected_key, damaged);
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, page.stage(&txn));
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, claims.GcPage.prepare(a, std.testing.io, &txn, binding));
        }
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            const selected_key = claims.inventoryKey(page.entries[0].address);
            const future = try a.dupe(u8, try txn.get(&selected_key));
            defer a.free(future);
            const canceled = (try @import("relational_index_cancellation_intent.zig").load(&txn)).?;
            std.mem.writeInt(u64, future[0..8], canceled.position.sequence, .little);
            // Re-sign the physical checksum to isolate cut-order validation
            // from the preceding ordinary checksum-corruption regression.
            var checksum = std.crypto.hash.Blake3.init(.{});
            checksum.update("antfly private claim provenance v1");
            checksum.update(&selected_key);
            var normalized = binding;
            normalized.phase = .reserved;
            checksum.update(&integrity.hash(&try normalized.encode()));
            checksum.update(&integrity.hash(page.entries[0].value));
            checksum.update(future[0..8]);
            checksum.final(future[8..40]);
            try txn.put(&selected_key, future);
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, claims.GcPage.prepare(a, std.testing.io, &txn, binding));
            const view = try claims.TransferView.init(&txn);
            try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, view.validateCompanion(&txn, .{ .address = page.entries[0].address, .kind = .claim }, integrity.hash(page.entries[0].value)));
        }
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            // Replacing provenance with another valid historical cut still
            // invalidates a prepared GC page, independently of its checksum.
            const selected_key = claims.inventoryKey(page.entries[0].address);
            const drift = try a.dupe(u8, try txn.get(&selected_key));
            defer a.free(drift);
            std.mem.writeInt(u64, drift[0..8], if (page.entries[0].sequence == original_cut) cleanup_clock - 2 else original_cut, .little);
            var checksum = std.crypto.hash.Blake3.init(.{});
            checksum.update("antfly private claim provenance v1");
            checksum.update(&selected_key);
            var normalized = binding;
            normalized.phase = .reserved;
            checksum.update(&integrity.hash(&try normalized.encode()));
            checksum.update(&integrity.hash(page.entries[0].value));
            checksum.update(drift[0..8]);
            checksum.final(drift[8..40]);
            try txn.write.?.put(&selected_key, drift);
            try std.testing.expectError(error.VersionConflict, page.stage(&txn));
        }
        collected += page.entries.len;
        // A crash before commit leaves the complete page retryable.
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            try page.stage(&txn);
        }
        try reopenPrivateClaimTestStore(a, restart);
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try page.stage(&txn);
            try txn.commit();
        }
        try reopenPrivateClaimTestStore(a, restart);
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(cleanup_clock, (try @import("../source_authority.zig").load(&read)).?.sequence);
            try std.testing.expectEqual(retained_start, if (try retained.load(&read)) |value| value.latest else @as(?u64, null));
            try std.testing.expectEqualSlices(u8, foreign_value, try read.get(&foreign_address.claimKey()));
            var reserved_binding = binding;
            reserved_binding.phase = .reserved;
            try std.testing.expectEqualSlices(u8, &try reserved_binding.encode(), &try (try jobs.load(&read)).?.encode());
        }
    }
    try std.testing.expectEqual(@as(usize, 5 + 2 * claims.max_claims), collected);
    try std.testing.expect(gc_pages >= 2);
    var read = try store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(&address.claimKey()));
    try std.testing.expectError(error.NotFound, read.get(&inventory_key));
    try std.testing.expectError(error.NotFound, read.get(&(try integrity.Address.init(allocation.generation, third.tuple)).claimKey()));
    _ = try claims.requireRegistered(&read, binding);
    try claims.requireRetired(&read, binding);
}

fn testOwnerRecovery(a: Allocator, store: *@import("../docstore.zig").DocStore, before: @import("schema_registry.zig").SchemaView, binding: @import("relational_index_owner_job.zig").Binding, reserved: bool) !void {
    const owner = @import("relational_index_owner_job.zig");
    const physical = @import("relational_index_catalog.zig");
    const pages = @import("relational_index_jobs.zig");
    const topology = @import("relational_integrity_topology.zig");
    var snapshot = try physical.Controller.loadSnapshot(a, store, before);
    defer if (snapshot) |*value| value.deinit();
    try std.testing.expectEqual(reserved or binding.physical_before != null, snapshot != null);
    if (snapshot) |value| try std.testing.expectEqual(@as(usize, @intFromBool(binding.physical_before != null)), value.plan.boundIndexes().len);
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, testOwnerRecoveryAllocations, .{&txn});
    }
    const Fault = enum { missing_job, missing_fence, namespace, integrity, schema, completed, progress_owner, progress_phase, candidate_missing, candidate_corrupt, native_admitted_missing, native_fenced_missing, native_reserved_missing, native_corrupt, native_clock_behind, authority_domain, raft_marker, native_padding, native_action, native_position_domain, native_namespace, native_zero_sequence, native_evidence, native_checksum, native_context, native_fence_order, inputs_missing, inputs_corrupt, inputs_substitution };
    for (std.enums.values(Fault)) |fault| {
        if (!reserved and (fault == .candidate_missing or fault == .candidate_corrupt)) continue;
        if (!reserved and (fault == .native_fenced_missing or fault == .native_reserved_missing or fault == .native_fence_order)) continue;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const ca = scratch.allocator();
        const progress_key = pages.progressKey(binding.index);
        const blob_key = physical.blobKey(binding.physical_candidate.blob_digest);
        const admitted_key = owner.nativeEffectKey(.admitted);
        const fenced_key = owner.nativeEffectKey(.fenced);
        const reserved_key = owner.nativeEffectKey(.reserved);
        const target: []const u8 = switch (fault) {
            .missing_job => owner.key,
            .inputs_missing, .inputs_corrupt, .inputs_substitution => inputs_mod.key,
            .missing_fence => topology.fence_key,
            .namespace => &@import("../internal_keys.zig").identity_namespace_key,
            .integrity => catalog.key,
            .schema => native_schema.schema_key,
            .completed => topology.receipt_key,
            .progress_owner, .progress_phase => &progress_key,
            .candidate_missing, .candidate_corrupt => &blob_key,
            .native_admitted_missing, .native_corrupt => &admitted_key,
            .native_padding, .native_action, .native_position_domain, .native_namespace, .native_zero_sequence, .native_evidence, .native_checksum, .native_context => &admitted_key,
            .native_fence_order => &fenced_key,
            .native_fenced_missing => &fenced_key,
            .native_reserved_missing => &reserved_key,
            .native_clock_behind, .authority_domain => @import("../source_authority.zig").key,
            .raft_marker => &@import("../internal_keys.zig").ordered_document_applied_entry_key,
        };
        const previous = store.get(ca, target) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const actual_owner = blk: {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            break :blk try pages.ownership(&txn);
        };
        var namespace_bytes: [24]u8 = undefined;
        var namespace = binding.fence.namespace;
        namespace.range_id += 1;
        @import("doc_identity.zig").encodeNamespace(&namespace_bytes, namespace);
        const fence_bytes = try binding.fence.encode();
        const source_bytes = (@import("../source_authority.zig").State{ .kind = if (fault == .authority_domain) .raft else .native, .namespace = @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), .sequence = 0 }).encode();
        const raft_bytes: [16]u8 = @splat(1);
        var tampered: [136]u8 = undefined;
        const substituted_inputs = if (fault == .inputs_substitution) blk: {
            const bytes = try ca.dupe(u8, previous.?);
            bytes[20] ^= 1;
            @memcpy(bytes[bytes.len - 32 ..], &inputs_mod.digest(bytes[0 .. bytes.len - 32]));
            break :blk bytes;
        } else null;
        switch (fault) {
            .native_padding, .native_action, .native_position_domain, .native_namespace, .native_zero_sequence, .native_evidence, .native_checksum, .native_context, .native_fence_order => {
                @memcpy(&tampered, previous.?);
                switch (fault) {
                    .native_padding => tampered[5] = 1,
                    .native_action => tampered[4] = @backingInt(owner.NativeEffect.reserved),
                    .native_position_domain => @memcpy(tampered[71..104], &try (@import("receipt_position.zig").Position{ .raft = .{ .term = 1, .index = 1 } }).encode()),
                    .native_namespace => std.mem.writeInt(u64, tampered[88..96], namespace.range_id, .little),
                    .native_zero_sequence => @memset(tampered[96..104], 0),
                    .native_evidence => tampered[39] ^= 1,
                    .native_checksum => tampered[104] ^= 1,
                    .native_context => tampered[7] ^= 1,
                    .native_fence_order => std.mem.writeInt(u64, tampered[96..104], 1, .little),
                    else => unreachable,
                }
                // Most faults have a valid physical checksum: recovery must
                // enforce the logical cut, not just trust intact bytes.
                if (fault != .native_checksum) std.crypto.hash.Blake3.hash(tampered[0..104], tampered[104..136], .{});
            },
            else => {},
        }
        const value: ?[]const u8 = switch (fault) {
            .missing_job, .missing_fence, .candidate_missing, .native_admitted_missing, .native_fenced_missing, .native_reserved_missing, .inputs_missing => null,
            .inputs_corrupt => "corrupt",
            .inputs_substitution => substituted_inputs.?,
            .namespace => &namespace_bytes,
            .integrity, .schema, .candidate_corrupt, .native_corrupt => "corrupt",
            .native_clock_behind, .authority_domain => &source_bytes,
            .raft_marker => &raft_bytes,
            .native_padding, .native_action, .native_position_domain, .native_namespace, .native_zero_sequence, .native_evidence, .native_checksum, .native_context, .native_fence_order => &tampered,
            .completed => &fence_bytes,
            .progress_owner, .progress_phase => try (pages.Progress{
                .id = binding.index,
                .owner = if (fault == .progress_phase) actual_owner else @splat(0),
                .comparison = @splat(0),
                .phase = if (fault == .progress_phase) .forward else .primary,
            }).encode(ca),
        };
        if (fault == .raft_marker) {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            try std.testing.expectError(error.OnlineSourceScopeChanged, txn.put(target, &raft_bytes));
        }
        {
            // Simulate persisted corruption below the document-store guards.
            // Ordinary writes must not be able to create these invalid cuts.
            var txn = try store.runtime_store.beginWrite();
            errdefer txn.abort();
            if (value) |bytes| try txn.put(target, bytes) else try txn.delete(target);
            try txn.commit();
        }
        const expected: anyerror = switch (fault) {
            .missing_job => error.PrivateIndexJobRequired,
            .inputs_missing, .inputs_corrupt, .inputs_substitution => error.InvalidPrivateIndexInputs,
            .missing_fence => error.IntegrityTopologyFenceMissing,
            .namespace => error.IdentityNamespaceMismatch,
            .integrity => error.IntegrityCatalogChanged,
            .schema => error.RelationalIndexCatalogSchemaMismatch,
            .completed => error.PrivateIndexPublicationRequired,
            .progress_owner, .progress_phase => error.InvalidRelationalIndexProgress,
            .candidate_missing => error.MissingRelationalIndexDefinitions,
            .candidate_corrupt => error.InvalidRelationalIndexCatalog,
            .native_admitted_missing, .native_fenced_missing, .native_reserved_missing, .native_corrupt, .native_clock_behind => error.InvalidPrivateIndexNativeReceipt,
            .native_padding, .native_action, .native_position_domain, .native_zero_sequence, .native_evidence, .native_checksum, .native_context, .native_fence_order => error.InvalidPrivateIndexNativeReceipt,
            .native_namespace => error.IdentityNamespaceMismatch,
            .authority_domain, .raft_marker => error.OnlineSourceScopeChanged,
        };
        try std.testing.expectError(expected, physical.Controller.loadSnapshot(a, store, before));
        if (reserved) switch (fault) {
            .inputs_missing, .inputs_corrupt, .inputs_substitution => {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                try std.testing.expectError(expected, owner.nativeReceipt(&txn, binding, .reserved));
            },
            .namespace, .schema, .native_admitted_missing, .native_fenced_missing, .native_reserved_missing, .native_corrupt, .native_clock_behind, .authority_domain, .raft_marker, .native_padding, .native_action, .native_position_domain, .native_namespace, .native_zero_sequence, .native_evidence, .native_checksum, .native_context, .native_fence_order => {
                // A worker already open at the old cut must reject corruption
                // too; restart validation alone cannot guard signing/writes.
                var txn = try store.beginReadTxn();
                defer txn.abort();
                try std.testing.expectError(expected, owner.requireReserved(&txn, binding));
                try std.testing.expectError(expected, owner.nativeReceipt(&txn, binding, .reserved));
            },
            else => {},
        };
        {
            var txn = try store.runtime_store.beginWrite();
            errdefer txn.abort();
            if (previous) |bytes| try txn.put(target, bytes) else try txn.delete(target);
            try txn.commit();
        }
    }
}

fn testOwnerRecoveryAllocations(a: Allocator, txn: *@import("../docstore.zig").DocStore.Txn) !void {
    const physical = @import("relational_index_catalog.zig");
    var active = try physical.load(a, txn);
    defer if (active) |*value| value.deinit();
    try @import("relational_index_owner_job.zig").verifyRecovery(a, txn, if (active) |*value| value else null);
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
    for (std.enums.values(@import("relational_index_owner_job.zig").NativeEffect)) |effect| {
        // Preparation may succeed before the durable cut is corrupted. Apply
        // must recheck it before writing even the first candidate or cursor.
        var page = (try jobs.Page.preparePrivate(a, std.testing.io, &core, binding, addition_plan, budget)).?;
        defer page.deinit();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(&@import("relational_index_owner_job.zig").nativeEffectKey(effect));
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, page.stage(&core, &manager, &txn));
        try std.testing.expectError(error.NotFound, txn.get(&jobs.progressKey(binding.index)));
    }
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
    try std.testing.expectEqual(addition_plan.plan.boundIndexes().len - 1, active.entries().len);
    try std.testing.expectEqual(before.version(), active.head.schema_version);
    var published = core.relational_indexes.acquire().?;
    defer published.deinit();
    try std.testing.expectEqual(addition_plan.plan.boundIndexes().len - 1, published.plan.boundIndexes().len);
}

fn testCancellationIntent(comptime Backend: type, comptime prior_phase: enum { absent, admitted, reserved }) !void {
    const a = std.testing.allocator;
    const jobs = @import("relational_index_owner_job.zig");
    const cancellation = @import("relational_index_cancellation_intent.zig");
    const physical = @import("relational_index_catalog.zig");
    const registry = @import("schema_registry.zig");
    const topology = @import("relational_integrity_topology.zig");
    const authority = @import("../source_authority.zig");
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
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var private_addition = try physical.PrivateAddition.init(a, before_view, after_view, null, (try after.relationalIndexDefinitions(arena.allocator())).?, "email_idx");
    defer private_addition.deinit();
    var hash: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(prior.value, &hash, .{});
    const fence: topology.Fence = .{ .role = .index_build, .transition_id = 7, .attempt = 1, .owner_group_id = 11, .peer_group_id = 11, .namespace = .{ .table_id = 101, .shard_id = 11, .range_id = 12 }, .catalog_digest = hash };
    const binding = try jobs.prepare(@splat(1), @splat(2), @splat(3), 0, fence, .native, &candidate, &private_addition);
    const intent = cancellation.Intent.fromBinding(binding);
    const claim_scope = try catalog.privateIndexAllocationFromReservation(candidate.reservation.value, binding.integrity_reserved, fence.namespace.table_id);
    const added_unique = candidate.next.catalog.find(.unique, "email_idx").?;
    try std.testing.expectEqual(added_unique.allocation, claim_scope.allocation);
    try std.testing.expectEqual(added_unique.generation, claim_scope.generation);
    try std.testing.expectError(error.InvalidIntegrityCatalog, catalog.privateIndexAllocationFromReservation(candidate.reservation.value, binding.integrity_reserved, fence.namespace.table_id + 1));
    try std.testing.expect(physical.Controller.isReservedMetadataKey(cancellation.key));
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, physical.Controller.validateExtraMetadata(&.{.{ .key = cancellation.key, .value = "forged" }}, &.{}));
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, physical.Controller.validateExtraMetadata(&.{}, &.{cancellation.key}));
    try std.testing.expect(physical.Controller.isReservedMetadataKey(jobs.cancellation_cleanup_key));
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, physical.Controller.validateExtraMetadata(&.{.{ .key = jobs.cancellation_cleanup_key, .value = "forged" }}, &.{}));
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, physical.Controller.validateExtraMetadata(&.{}, &.{jobs.cancellation_cleanup_key}));
    var backend = Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "cancel-intent" });
    defer runtime.deinit();
    var store = try @import("../docstore.zig").DocStore.openRuntime(a, &runtime);
    defer store.close();
    _ = try native_schema.saveSchema(&store, a, before_schema);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(catalog.key, prior.value);
        const bytes = @import("online_source_contract.zig").namespaceBytes(fence.namespace);
        try @import("doc_identity.zig").writeNamespaceTxn(&txn, fence.namespace);
        try authority.bind(&txn, .native, bytes);
        try txn.commit();
    }
    var manager = try @import("../transactions.zig").TxnManager.init(a, &store);
    defer manager.deinit();
    if (prior_phase != .reserved) try manager.initTransaction(@splat(9), 1);
    if (prior_phase != .absent) {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        _ = try jobs.stageBegin(&txn, binding, &candidate.inputs);
        if (prior_phase == .reserved) {
            _ = try jobs.stageNativeFenced(&txn, &manager, binding);
            _ = try jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition);
        }
        try txn.commit();
    }
    const old_sequence: u64 = if (prior_phase == .absent) 0 else if (prior_phase == .admitted) 1 else 3;
    const Pages = @import("relational_index_jobs.zig");
    const PageCore = struct {
        alloc: Allocator,
        store: *@import("../docstore.zig").DocStore,
        relational_indexes: physical.Controller,
        identity_namespace: @import("doc_identity_namespace.zig").Namespace,
        before: registry.SchemaView,
        pub fn schemaNamespaceGeneration(_: *@This()) u64 {
            return 1;
        }
        pub fn acquireSchemaVersionView(self: *@This(), version: u32) !?registry.SchemaView {
            return if (version == self.before.version()) self.before.clone() else null;
        }
        pub fn initTxnManager(self: *@This()) !@import("../transactions.zig").TxnManager {
            return @import("../transactions.zig").TxnManager.init(self.alloc, self.store);
        }
    };
    var page_core: ?PageCore = if (prior_phase == .reserved) .{ .alloc = a, .store = &store, .relational_indexes = try physical.Controller.init(a, std.testing.io, &store, before_view), .identity_namespace = fence.namespace, .before = before_view } else null;
    defer if (page_core) |*value| value.relational_indexes.deinit();
    var prepared_page: ?Pages.Page = if (page_core) |*value| try Pages.Page.preparePrivate(a, std.testing.io, value, binding, &private_addition, .{ .records = 1, .time_ns = std.time.ns_per_s }) else null;
    defer if (prepared_page) |*value| value.deinit();
    if (prior_phase == .reserved) try std.testing.expect(prepared_page != null);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const staged = try jobs.stageCancellationIntentWithReplay(&txn, intent, null);
        try std.testing.expect(!staged.duplicate);
        try std.testing.expectEqual(old_sequence + 1, staged.record.position.sequence);
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expect(try cancellation.load(&txn) == null);
        try std.testing.expectEqual(old_sequence, (try authority.load(&txn)).?.sequence);
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.OnlineSourceScopeChanged, jobs.stageCancellationIntentWithReplay(&txn, intent, .{ .namespace = fence.namespace, .sequence = old_sequence + 2 }));
    }
    var committed: cancellation.Record = undefined;
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        committed = (try jobs.stageCancellationIntentWithReplay(&txn, intent, .{ .namespace = fence.namespace, .sequence = old_sequence + 1 })).record;
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const duplicate = try jobs.stageCancellationIntentWithReplay(&txn, intent, committed.position);
        try std.testing.expect(duplicate.duplicate);
        try std.testing.expectEqualDeep(committed, duplicate.record);
        try std.testing.expectEqual(committed.position.sequence, (try authority.load(&txn)).?.sequence);
        var foreign = intent;
        foreign.owner_digest[0] ^= 1;
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, jobs.stageCancellationIntentWithReplay(&txn, foreign, null));
        foreign = intent;
        foreign.fence.namespace.range_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, jobs.stageCancellationIntentWithReplay(&txn, foreign, null));
        try std.testing.expectError(error.PrivateIndexPublicationCanceled, jobs.stageBegin(&txn, binding, &candidate.inputs));
        try std.testing.expectError(error.PrivateIndexPublicationCanceled, jobs.stageNativeFenced(&txn, &manager, binding));
        try std.testing.expectError(error.PrivateIndexPublicationCanceled, jobs.stageReservation(&txn, &manager, binding, &candidate, &private_addition));
        try std.testing.expectError(error.PrivateIndexPublicationCanceled, jobs.requireReserved(&txn, binding));
        if (prepared_page) |*page| {
            // This page was independently prepared before intent committed.
            // Apply must reject it before writing candidate data or progress.
            try std.testing.expectError(error.PrivateIndexPublicationCanceled, page.stage(&page_core.?, &manager, &txn));
            try std.testing.expectError(error.NotFound, txn.get(&Pages.progressKey(binding.index)));
        }
        try std.testing.expectError(error.IntegrityTopologyBusy, topology.requireUnfenced(&txn));
        try std.testing.expectError(error.PrivateIndexCancellationRequired, topology.stageCancel(&txn, fence));
        var later = intent;
        later.id[0] ^= 1;
        later.fence.admission_epoch += 1;
        try std.testing.expectError(if (prior_phase == .absent) error.PrivateIndexCancellationPending else error.PrivateIndexOwnerJobChanged, jobs.stageCancellationIntentWithReplay(&txn, later, null));
        try testOwnerRecoveryAllocations(a, &txn);
    }
    if (prior_phase != .reserved) {
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            _ = try jobs.stageCanceledBeginWithReplay(&txn, binding, &candidate.inputs, null);
            try std.testing.expectError(error.TransactionTopologyBusy, jobs.stageCanceledFencedWithReplay(&txn, &manager, binding, null));
        }
        try manager.resolveIntents(@splat(9), .aborted, 2);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectEqual(prior_phase == .absent, try jobs.stageCanceledBeginWithReplay(&txn, binding, &candidate.inputs, null));
        const drained = try jobs.stageCanceledFencedWithReplay(&txn, &manager, binding, null);
        try std.testing.expectEqual(.fenced, drained.action);
        _ = try jobs.stageCanceledReservationWithReplay(&txn, &manager, binding, &candidate, &private_addition, null);
        try txn.commit();
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try testOwnerRecoveryAllocations(a, &txn);
        try std.testing.expectEqual(.reserved, (try jobs.load(&txn)).?.phase);
        try std.testing.expectEqualSlices(u8, candidate.reservation.value, try txn.get(catalog.key));
        try std.testing.expectError(error.PrivateIndexPublicationCanceled, jobs.requireReserved(&txn, binding));
        // Intent is neither a completed epoch nor cleanup evidence. The fence
        // remains closed, and no canceled publication ACK has been fabricated.
        try std.testing.expect(try topology.completed(&txn) == null);
        try std.testing.expect((try topology.current(&txn)).?.eql(fence));
    }
    const gc = @import("relational_index_gc.zig");
    const records = @import("relational_index_records.zig");
    var cleanup_core: PageCore = .{ .alloc = a, .store = &store, .relational_indexes = try physical.Controller.init(a, std.testing.io, &store, before_view), .identity_namespace = fence.namespace, .before = before_view };
    defer cleanup_core.relational_indexes.deinit();
    var sequence: u64 = undefined;
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        sequence = (try authority.load(&txn)).?.sequence;
        try std.testing.expectError(error.PrivateIndexCancellationRequired, jobs.requireCancellationArtifactsRetired(&txn, binding));
        try std.testing.expectError(error.OnlineSourceScopeChanged, jobs.stageCancellationCleanupWithReplay(&txn, &manager, binding, .{ .namespace = fence.namespace, .sequence = sequence + 2 }));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        // Public admission's full-queue behavior is covered by GC's capacity
        // fixture. Cancellation must be independent of that inventory: fail
        // immediately if it ever tries to scan the queue. This avoids building
        // 8,192 unrelated retirement records in every physical-owner scenario.
        const PointOnly = struct {
            inner: *@import("../docstore.zig").DocStore.Txn,
            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                return self.inner.get(key);
            }
            pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
                try self.inner.put(key, value);
            }
            pub fn openCursor(_: *@This()) !@import("../docstore.zig").DocStore.Txn.CursorAdapter {
                return error.UnexpectedCancellationAdmissionScan;
            }
        };
        var points: PointOnly = .{ .inner = &txn };
        const queued = try jobs.stageCancellationCleanupWithReplay(&points, &manager, binding, null);
        try std.testing.expectEqual(sequence + 1, queued.position.sequence);
    }
    var queued: jobs.CancellationCleanup = undefined;
    const forward_prefix = try records.forwardPrefix(binding.index);
    const unrelated = records.Id{ .generation = binding.index.generation + 1000, .slot = 0 };
    const unrelated_key = try records.forwardPrefix(unrelated);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(&gc.key(binding.index)));
        try std.testing.expectEqual(sequence, (try authority.load(&txn)).?.sequence);
        // More than one GC page, with atomic reverse/ownership companions.
        // No forward value decoding is needed to retire a canceled generation.
        for (0..300) |offset| {
            var forward: [records.forward_prefix_len + 2]u8 = undefined;
            @memcpy(forward[0..records.forward_prefix_len], &forward_prefix);
            std.mem.writeInt(u16, forward[records.forward_prefix_len..], @intCast(offset), .big);
            try txn.put(&forward, "private");
            var document_buffer: [32]u8 = undefined;
            const document = try std.fmt.bufPrint(&document_buffer, "doc-{d}", .{offset});
            var reverse = std.ArrayList(u8).empty;
            defer reverse.deinit(a);
            var ownership = std.ArrayList(u8).empty;
            defer ownership.deinit(a);
            try records.appendReverseKey(a, &reverse, binding.index, document);
            try records.appendOwnershipFromReverse(a, &ownership, reverse.items);
            try txn.put(reverse.items, "private");
            try txn.put(ownership.items, "");
        }
        try txn.put(&unrelated_key, "keep");
        queued = try jobs.stageCancellationCleanupWithReplay(&txn, &manager, binding, .{ .namespace = fence.namespace, .sequence = sequence + 1 });
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const retry = try jobs.stageCancellationCleanupWithReplay(&txn, &manager, binding, queued.position);
        try std.testing.expect(retry.duplicate);
        try std.testing.expectEqual(queued.position, retry.position);
        try std.testing.expectError(error.PrivateIndexCancellationPending, jobs.requireCancellationArtifactsRetired(&txn, binding));
        try testOwnerRecoveryAllocations(a, &txn);
        const stored_cleanup = try jobs.CancellationCleanupRecord.decode(try txn.get(jobs.cancellation_cleanup_key));
        try std.testing.expectEqual(claim_scope.allocation, stored_cleanup.claim_allocation);
        try std.testing.expectEqual(claim_scope.generation, try stored_cleanup.claimGeneration());
        const cleanup_bytes = try stored_cleanup.encode();
        for (0..cleanup_bytes.len) |length| try std.testing.expectError(error.InvalidPrivateIndexCancellation, jobs.CancellationCleanupRecord.decode(cleanup_bytes[0..length]));
        var extended: [jobs.CancellationCleanupRecord.encoded_len + 1]u8 = undefined;
        @memcpy(extended[0..cleanup_bytes.len], &cleanup_bytes);
        extended[cleanup_bytes.len] = 0;
        try std.testing.expectError(error.InvalidPrivateIndexCancellation, jobs.CancellationCleanupRecord.decode(&extended));
        var no_allocation = cleanup_bytes;
        @memset(no_allocation[376..384], 0);
        std.crypto.hash.Blake3.hash(no_allocation[0..384], no_allocation[384..416], .{});
        try std.testing.expectError(error.InvalidPrivateIndexCancellation, jobs.CancellationCleanupRecord.decode(&no_allocation));
        for ([_]usize{ 0, 5, 375, 376, 415 }) |offset| {
            var corrupt = cleanup_bytes;
            corrupt[offset] ^= 1;
            try std.testing.expectError(error.InvalidPrivateIndexCancellation, jobs.CancellationCleanupRecord.decode(&corrupt));
        }
        var invalid_cleanup = stored_cleanup;
        invalid_cleanup.position = committed.position;
        try txn.put(jobs.cancellation_cleanup_key, &try invalid_cleanup.encode());
        try std.testing.expectError(error.InvalidPrivateIndexCancellation, testOwnerRecoveryAllocations(a, &txn));
        invalid_cleanup = stored_cleanup;
        invalid_cleanup.claim_allocation += 1;
        try txn.put(jobs.cancellation_cleanup_key, &try invalid_cleanup.encode());
        try std.testing.expectError(error.InvalidPrivateIndexCancellation, testOwnerRecoveryAllocations(a, &txn));
        invalid_cleanup = stored_cleanup;
        invalid_cleanup.intent.owner_digest[0] ^= 1;
        try txn.put(jobs.cancellation_cleanup_key, &try invalid_cleanup.encode());
        try std.testing.expectError(error.InvalidPrivateIndexCancellation, testOwnerRecoveryAllocations(a, &txn));
    }
    var cleanup_pages: usize = 0;
    while (try gc.Page.prepare(a, std.testing.io, &cleanup_core)) |prepared_gc| {
        var page = prepared_gc;
        defer page.deinit();
        cleanup_pages += 1;
        try std.testing.expect(cleanup_pages <= 8);
        try std.testing.expect(page.records_scanned <= 256);
        try page.commit(&cleanup_core);
    }
    try std.testing.expect(cleanup_pages >= 4);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try jobs.requireCancellationArtifactsRetired(&txn, binding);
        const retry = try jobs.stageCancellationCleanupWithReplay(&txn, &manager, binding, queued.position);
        try std.testing.expect(retry.duplicate);
        try std.testing.expectError(error.NotFound, txn.get(&gc.key(binding.index)));
        try std.testing.expectEqualStrings("keep", try txn.get(&unrelated_key));
        for (0..300) |offset| {
            var document_buffer: [32]u8 = undefined;
            const document = try std.fmt.bufPrint(&document_buffer, "doc-{d}", .{offset});
            var reverse = std.ArrayList(u8).empty;
            defer reverse.deinit(a);
            try records.appendReverseKey(a, &reverse, binding.index, document);
            try std.testing.expectError(error.NotFound, txn.get(reverse.items));
        }
        try std.testing.expectEqual(queued.position.sequence, (try authority.load(&txn)).?.sequence);
        // Physical cleanup does not consume the epoch or acknowledge cancel.
        try std.testing.expect(try topology.completed(&txn) == null);
        try std.testing.expect((try topology.current(&txn)).?.eql(fence));
        try testOwnerRecoveryAllocations(a, &txn);
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var future = committed;
        future.position.sequence = (try authority.load(&txn)).?.sequence + 1;
        try txn.put(cancellation.key, &try future.encode());
        try std.testing.expectError(error.InvalidPrivateIndexCancellation, testOwnerRecoveryAllocations(a, &txn));
    }
}

test "relational index candidate cancellation intent blocks delayed normal work but permits exact drain and burns" {
    inline for (.{ .absent, .admitted, .reserved }) |phase| {
        try testCancellationIntent(@import("../mem_backend.zig").Backend, phase);
        try testCancellationIntent(@import("../lsm_backend.zig").Backend, phase);
    }
}

test "relational index candidate owner binding fences drains and reserves both catalogs atomically on memory and LSM" {
    inline for (.{ false, true }) |existing| {
        try testOwnerJobAdmission(@import("../mem_backend.zig").Backend, existing, false);
        try testOwnerJobAdmission(@import("../lsm_backend.zig").Backend, existing, false);
    }
}

test "relational index candidate native replay preserves original cuts and rejects clock gaps on memory and LSM" {
    inline for (.{ false, true }) |existing| {
        try testOwnerJobAdmission(@import("../mem_backend.zig").Backend, existing, true);
        try testOwnerJobAdmission(@import("../lsm_backend.zig").Backend, existing, true);
    }
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
