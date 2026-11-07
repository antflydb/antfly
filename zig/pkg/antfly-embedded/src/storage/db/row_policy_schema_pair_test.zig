// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Actual owner-local journal, retained-reader and cold policy/schema tests.
const std = @import("std");
const DB = @import("db.zig").DB;
const policies = @import("../../system_catalog/policies.zig");
const schema = @import("../schema.zig");
const bundle = @import("row_policy_bundle.zig");
const authority = @import("../../usermgr/row_policy_authority.zig");
const background = @import("../background_runtime.zig");
const A = std.mem.Allocator;
const before_json =
    \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
;
const after_json =
    \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
;
const secret = "coupled-policy-real-owner-signing-secret";
const issuer = "coupled-policy-test";
const namespace: @import("db.zig").DocIdentityNamespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
const owner_group: u64 = 17;

fn typedDigest(a: A, json: []const u8) ![32]u8 {
    var public = try @import("../../schema/mod.zig").parseValidatedTableSchema(a, json);
    defer public.deinit(a);
    const native = try @import("../../schema/mod.zig").deriveRuntimeTableSchema(a, public);
    defer schema.freeSchema(a, native);
    const bytes = try schema.serializeSchema(a, native);
    defer a.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    return digest;
}
fn recordDigest(json: []const u8, read_json: []const u8) [32]u8 {
    // Same actual record projection as policyTableSchemaRecordDigest.
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(json);
    hash.update(&.{0});
    hash.update(read_json);
    hash.update(&.{0});
    hash.update(""); // no indexes in this actual owner fixture
    hash.update(&.{0});
    hash.update("0"); // no storage migration
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn pair(a: A) !policies.SchemaPair {
    return .{ .before_schema_json = before_json, .after_schema_json = after_json, .before_schema_digest = try typedDigest(a, before_json), .after_schema_digest = try typedDigest(a, after_json), .before_record_digest = recordDigest(before_json, ""), .staged_record_digest = recordDigest(after_json, before_json), .after_record_digest = recordDigest(after_json, ""), .before_policy_generation = 1, .before_catalog_epoch = 2 };
}
fn bindVerifier(db: *DB) void {
    db.local_execution.row_policy_authority_secret = secret;
    db.local_execution.row_policy_authority_issuer = issuer;
    db.local_execution.row_policy_table_name = "rows";
}
fn open(a: A, path: []const u8, runtime: ?*background.BackendRuntime) !DB {
    var db = try DB.open(a, path, .{ .identity_namespace = namespace, .backend_runtime = runtime, .start_index_workers = false, .start_independent_maintenance = false, .start_optional_runtimes = false });
    bindVerifier(&db);
    return db;
}
fn bundleBytes(a: A, generation: u64, phase: policies.Publication.Phase, coupled: bool) ![]u8 {
    const version: u32 = if (coupled) 2 else 1;
    const digest = try typedDigest(a, if (coupled) after_json else before_json);
    const record: policies.Record = .{ .id = 1, .generation = generation, .table_id = 7, .schema_version = version, .schema_digest = digest, .name = "visible", .commands = .{ .select = true }, .roles = &.{"PUBLIC"}, .using = .{ .instructions = &.{.{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } }}, .root = 0 } };
    return std.json.Stringify.valueAlloc(a, policies.InstallSnapshot{ .table_id = 7, .schema_version = version, .schema_digest = digest, .policy_generation = generation, .catalog_epoch = generation + 1, .phase = phase, .schema_pair = if (coupled) try pair(a) else null, .records = &.{record}, .settings = &.{} }, .{});
}
fn request(a: A, generation: u64, phase: policies.Publication.Phase) !policies.InstallRequest {
    return .{ .table_id = 7, .expected_generation = generation, .expected_catalog_epoch = generation + 1, .expected_phase = phase, .owner_group_id = owner_group, .expected_descriptor_digest = try (policies.OwnerDescriptor{ .table_id = 7, .group_id = owner_group, .shard_id = namespace.shard_id, .range_id = namespace.range_id, .schema_version = if (generation == 1) 1 else 2, .schema_digest = try typedDigest(a, if (generation == 1) before_json else after_json), .range_start = "", .range_end = "" }).digest() };
}
fn installInitial(a: A, db: *DB) !void {
    try db.setSchemaJson(a, before_json);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":\"a\"}" }}, .sync_level = .write });
    inline for (.{ policies.Publication.Phase.pending_install, policies.Publication.Phase.serving_install }, 0..) |phase, index| {
        const encoded = try bundleBytes(a, 1, phase, false);
        defer a.free(encoded);
        _ = try db.applyReplicatedRowPolicyPublication(encoded, try request(a, 1, phase), .{ .term = 3, .index = 11 + index });
        const receipt = try db.loadRowPolicyReceipt(1, phase);
        try std.testing.expectEqual(@as(u64, 3), receipt.applied_term);
        try std.testing.expectEqual(@as(u64, 11 + index), receipt.applied_index);
    }
    try expectPair(a, db, 1);
}
fn token(a: A, generation: u64) ![]u8 {
    return authority.sign(a, secret, issuer, .{ .principal = "alice", .roles = &.{"PUBLIC"}, .auth_revision = 1 }, .{ .table_id = 7, .table = "rows", .database = "main", .policy_generation = generation, .catalog_epoch = generation + 1 }, @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
}
fn expectPair(a: A, db: *DB, version: u32) !void {
    try std.testing.expectEqual(version, db.core.table_catalog.active_schema_version);
    try std.testing.expectEqual(version, db.core.schema.?.version);
    const actual_json = (try db.getSchemaJson(a)) orelse return error.MissingCoupledSchema;
    defer a.free(actual_json);
    try std.testing.expectEqualStrings(if (version == 1) before_json else after_json, actual_json);
    const stored = try db.core.store.get(a, bundle.key);
    defer a.free(stored);
    var installed = try std.json.parseFromSlice(policies.InstallSnapshot, a, stored, .{});
    defer installed.deinit();
    try std.testing.expectEqual(version, installed.value.schema_version);
    try std.testing.expectEqual(@as(u64, version), installed.value.policy_generation);
    try std.testing.expectEqual(@as(u64, version + 1), installed.value.catalog_epoch);
    const actual_digest = try typedDigest(a, actual_json);
    try std.testing.expectEqualSlices(u8, &actual_digest, &installed.value.schema_digest);
}
fn expectRow(a: A, db: *DB, generation: u64) !void {
    const signed = try token(a, generation);
    defer a.free(signed);
    var row = (try db.lookup(a, "a", .{ .include_primary_digest = true, .row_policy_principal_proof = signed, .row_policy_database = "main" })) orelse return error.MissingCoupledRow;
    defer row.deinit(a);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, row.json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("a", parsed.value.object.get("id").?.string);
    try std.testing.expect(row.expected_content_digest != null);
}
fn stageCandidate(a: A, db: *DB) !void {
    const encoded = try bundleBytes(a, 2, .pending_install, true);
    defer a.free(encoded);
    const actual = (try db.applyReplicatedRowPolicyPublication(encoded, try request(a, 2, .pending_install), .{ .term = 3, .index = 20 })) orelse return error.MissingCandidateReceipt;
    try std.testing.expectEqual(@as(u64, 20), actual.applied_index);
    const observed = try db.loadRowPolicyReceipt(2, .pending_install);
    try std.testing.expectEqualDeep(actual, observed);
    try expectPair(a, db, 1);
    try expectRow(a, db, 1);
}

test "row-policy coupled schema retains real readers and recovers exact serving intent without replay" {
    const a = std.testing.allocator;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("coupled-policy-reader");
    defer directory.cleanup();
    const serving = try bundleBytes(a, 2, .serving_install, true);
    defer a.free(serving);
    const install = try request(a, 2, .serving_install);
    {
        var db = try open(a, directory.path(), null);
        defer db.close();
        try installInitial(a, &db);
        var manager = try db.core.initTxnManager();
        defer manager.deinit();
        const transaction: @import("types.zig").TxnId = @splat(44);
        try manager.initTransaction(transaction, 100);
        try manager.writeIntentsExtraBatch(transaction, &.{.{ .key = "held", .value = "{\"id\":\"held\"}" }}, &.{}, .{ .schema_binding = .{ .version = 1 } });
        try std.testing.expect(try manager.hasSchemaLeases());
        const signed = try token(a, 1);
        defer a.free(signed);
        const reader = try db.openRelationalReadSession(a, "", "", .{ .row_policy_principal_proof = signed, .row_policy_database = "main", .relational_query = .{ .fields = &.{"id"} } });
        var reader_live = true;
        defer if (reader_live) reader.deinit();
        try stageCandidate(a, &db);
        try std.testing.expect((try db.applyReplicatedRowPolicyPublication(serving, install, .{ .term = 3, .index = 21 })) == null);
        try expectPair(a, &db, 1);
        try std.testing.expectError(error.RowPolicyReadersActive, db.loadRowPolicyReceipt(2, .serving_install));
        // Duplicate applied entry cannot bypass either the policy or schema lease.
        try std.testing.expect((try db.applyReplicatedRowPolicyPublication(serving, install, .{ .term = 3, .index = 21 })) == null);
        try expectPair(a, &db, 1);
        reader.deinit();
        reader_live = false;
        // Draining read cursors alone cannot erase durable schema custody.
        try std.testing.expectError(error.RowPolicyReadersActive, db.loadRowPolicyReceipt(2, .serving_install));
        try expectPair(a, &db, 1);
        try manager.resolveIntents(transaction, .aborted, 101);
        try std.testing.expect(!try manager.hasSchemaLeases());
        // Intentionally lose the serving response: the durable intent, not
        // another metadata snapshot or VM invocation, must recover cold.
    }
    {
        var db = try open(a, directory.path(), null);
        defer db.close();
        const receipt = try db.loadRowPolicyReceipt(2, .serving_install);
        try std.testing.expectEqual(@as(u64, 3), receipt.applied_term);
        try std.testing.expectEqual(@as(u64, 21), receipt.applied_index);
        try expectPair(a, &db, 2);
        try expectRow(a, &db, 2);
        const generation = db.core.table_catalog.generation;
        try std.testing.expectEqualDeep(receipt, (try db.applyReplicatedRowPolicyPublication(serving, install, .{ .term = 3, .index = 21 })).?);
        try std.testing.expectEqual(generation, db.core.table_catalog.generation);
        try std.testing.expectError(error.NotFound, db.core.store.get(a, bundle.candidate_key));
        try std.testing.expectError(error.NotFound, db.core.store.get(a, bundle.pending_key));
    }
    {
        var db = try open(a, directory.path(), null);
        defer db.close();
        try expectPair(a, &db, 2);
        try expectRow(a, &db, 2);
        try std.testing.expectEqual(@as(u64, 21), (try db.loadRowPolicyReceipt(2, .serving_install)).applied_index);
    }
}

test "row-policy coupled schema candidate survives cold open and rejects changed replay bytes" {
    const a = std.testing.allocator;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("coupled-policy-candidate");
    defer directory.cleanup();
    const pending = try bundleBytes(a, 2, .pending_install, true);
    defer a.free(pending);
    const install = try request(a, 2, .pending_install);
    {
        var db = try open(a, directory.path(), null);
        defer db.close();
        try installInitial(a, &db);
        try stageCandidate(a, &db);
    }
    {
        var db = try open(a, directory.path(), null);
        defer db.close();
        try expectPair(a, &db, 1);
        try expectRow(a, &db, 1);
        const receipt = try db.loadRowPolicyReceipt(2, .pending_install);
        const catalog_generation = db.core.table_catalog.generation;
        try std.testing.expectEqualDeep(receipt, (try db.applyReplicatedRowPolicyPublication(pending, install, .{ .term = 3, .index = 20 })).?);
        try std.testing.expectEqual(catalog_generation, db.core.table_catalog.generation);
        var changed = try std.json.parseFromSlice(policies.InstallSnapshot, a, pending, .{});
        defer changed.deinit();
        // Keep the exact schema/pair/owner descriptor valid. A different
        // policy name is a valid body substitution at the already-owned cut,
        // so only receipt deduplication may reject it.
        var substituted = changed.value.records[0];
        substituted.name = "changed_visible";
        const records = [_]policies.Record{substituted};
        changed.value.records = &records;
        try changed.value.validateShape();
        const altered = try std.json.Stringify.valueAlloc(a, changed.value, .{});
        defer a.free(altered);
        try std.testing.expectError(error.InvalidRowPolicyReceipt, db.applyReplicatedRowPolicyPublication(altered, install, .{ .term = 3, .index = 20 }));
        try expectPair(a, &db, 1);
        const serving = try bundleBytes(a, 2, .serving_install, true);
        defer a.free(serving);
        // An immutable historical epoch reference is not a policy read or a
        // durable transaction lease. The core's own prepared base view must
        // likewise not make publication wait for itself.
        var retained_epoch = db.core.acquireSchemaView() orelse return error.MissingCoupledSchema;
        defer retained_epoch.release();
        try std.testing.expectEqual(@as(u32, 1), retained_epoch.version());
        _ = try db.applyReplicatedRowPolicyPublication(serving, try request(a, 2, .serving_install), .{ .term = 3, .index = 21 });
        _ = try db.loadRowPolicyReceipt(2, .serving_install);
        try std.testing.expectEqual(@as(u32, 1), retained_epoch.version());
        try expectPair(a, &db, 2);
        try expectRow(a, &db, 2);
    }
}

const Fault = struct {
    const Kind = enum(u8) { none, no_space, canceled };
    var armed: std.atomic.Value(u8) = .init(0);
    var hits: std.atomic.Value(u32) = .init(0);
    fn write(userdata: ?*anyopaque, file: std.Io.File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) std.Io.File.WritePositionalError!usize {
        const fault: Kind = @fromBackingInt(@intCast(armed.swap(0, .acq_rel)));
        if (fault != .none) {
            _ = hits.fetchAdd(1, .monotonic);
            return if (fault == .no_space) error.NoSpaceLeft else error.Canceled;
        }
        return std.testing.io.vtable.fileWritePositional(userdata, file, header, data, splat, offset);
    }
};

test "row-policy coupled schema actual primary NoSpace and cancellation retain the exact candidate across cold recovery" {
    const a = std.testing.allocator;
    var vtable = std.testing.io.vtable.*;
    vtable.fileWritePositional = Fault.write;
    var io = std.testing.io;
    io.vtable = &vtable;
    var runtime = try background.BackendRuntimeHandle.init(a, .{ .backend = .manual, .borrowed_io = .{ .general = io }, .filesystem_io = io });
    defer runtime.deinit();
    inline for (.{ Fault.Kind.no_space, Fault.Kind.canceled }) |fault| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("coupled-policy-fault");
        defer directory.cleanup();
        const serving = try bundleBytes(a, 2, .serving_install, true);
        defer a.free(serving);
        const install = try request(a, 2, .serving_install);
        {
            Fault.armed.store(0, .release);
            var db = try open(a, directory.path(), runtime.runtime);
            defer {
                Fault.armed.store(0, .release);
                db.close();
            }
            try installInitial(a, &db);
            const signed = try token(a, 1);
            defer a.free(signed);
            const reader = try db.openRelationalReadSession(a, "", "", .{ .row_policy_principal_proof = signed, .row_policy_database = "main", .relational_query = .{ .fields = &.{"id"} } });
            var reader_live = true;
            defer if (reader_live) reader.deinit();
            try stageCandidate(a, &db);
            try std.testing.expect((try db.applyReplicatedRowPolicyPublication(serving, install, .{ .term = 3, .index = 21 })) == null);
            reader.deinit();
            reader_live = false;
            // BorrowedIo deliberately selects IoStorage, whose WAL append
            // propagates writePositionalAll errors without std.log.err. The
            // exact returned cause and one real I/O hit remain mandatory;
            // no expected-error-log allowance masks unrelated diagnostics.
            Fault.hits.store(0, .release);
            Fault.armed.store(@backingInt(fault), .release);
            if (fault == .no_space)
                try std.testing.expectError(error.NoSpaceLeft, db.loadRowPolicyReceipt(2, .serving_install))
            else
                try std.testing.expectError(error.Canceled, db.loadRowPolicyReceipt(2, .serving_install));
            try std.testing.expectEqual(@as(u32, 1), Fault.hits.load(.acquire));
            try expectPair(a, &db, 1);
            const candidate = try db.core.store.get(a, bundle.candidate_key);
            defer a.free(candidate);
            var saved = try std.json.parseFromSlice(policies.InstallSnapshot, a, candidate, .{});
            defer saved.deinit();
            try std.testing.expectEqual(@as(u32, 2), saved.value.schema_version);
            const expected_pair = try pair(a);
            const expected_digest = try expected_pair.digest(a);
            const saved_digest = try saved.value.schema_pair.?.digest(a);
            try std.testing.expectEqualSlices(u8, &expected_digest, &saved_digest);
        }
        {
            var db = try open(a, directory.path(), runtime.runtime);
            defer db.close();
            const receipt = try db.loadRowPolicyReceipt(2, .serving_install);
            try std.testing.expectEqual(@as(u64, 21), receipt.applied_index);
            try expectPair(a, &db, 2);
            try expectRow(a, &db, 2);
            try std.testing.expectEqualDeep(receipt, (try db.applyReplicatedRowPolicyPublication(serving, install, .{ .term = 3, .index = 21 })).?);
        }
    }
}

fn oldRowPolicyBundle(a: A, generation: u64, phase: policies.Publication.Phase, coupled: bool) ![]u8 {
    const raw = try bundleBytes(a, generation, phase, coupled);
    defer a.free(raw);
    var parsed = try std.json.parseFromSlice(policies.InstallSnapshot, a, raw, .{});
    defer parsed.deinit();
    // Both USING and WITH CHECK inspect the actual id datum, not a constant.
    const visible: policies.Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .string }, .operation = .{ .column = 0 } },
        .{ .type = .{ .kind = .string }, .operation = .{ .literal = .{ .string = "blocked" } } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .binary = .{ .op = .neq, .left = 0, .right = 1 } } },
    }, .root = 2 };
    var record = parsed.value.records[0];
    record.commands = .{ .select = true, .update = true, .delete = true };
    record.using = visible;
    record.with_check = visible;
    const records = [_]policies.Record{record};
    parsed.value.records = &records;
    try parsed.value.validateShape();
    return std.json.Stringify.valueAlloc(a, parsed.value, .{});
}
fn physicalRow(a: A, db: *DB, key: []const u8) ![]u8 {
    const storage = @import("relational_store.zig");
    const encoded = try storage.keyAlloc(a, key);
    defer a.free(encoded);
    return db.core.store.get(a, encoded);
}
fn expectPhysicalRow(a: A, db: *DB, key: []const u8, version: u32, expected: ?[]const u8) !void {
    const bytes = try physicalRow(a, db, key);
    defer a.free(bytes);
    try std.testing.expectEqual(version, try @import("relational_store.zig").rowSchemaVersion(bytes));
    if (expected) |before| try std.testing.expectEqualSlices(u8, before, bytes);
}
fn expectProtectedScan(a: A, db: *DB, expected: []const []const u8) !void {
    const proof = try token(a, 2);
    defer a.free(proof);
    var result = try db.scan(a, "", "", .{ .include_documents = true, .relational_query = .{ .schema_version = 2, .fields = &.{"id"} }, .row_policy_principal_proof = proof, .row_policy_database = "main" });
    defer result.deinit(a);
    try std.testing.expectEqual(expected.len, result.documents.len);
    for (expected, result.documents) |id, document| {
        try std.testing.expectEqualStrings(id, document.id);
        var value = try std.json.parseFromSlice(std.json.Value, a, document.json, .{});
        defer value.deinit();
        try std.testing.expectEqualStrings(id, value.value.object.get("id").?.string);
    }
}

test "row-policy coupled schema preserves physical old rows through protected cold reads scans and checked mutations" {
    const a = std.testing.allocator;
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("coupled-policy-old-row");
    defer directory.cleanup();
    var originals: [3]?[]u8 = @splat(null);
    defer for (originals) |bytes| if (bytes) |owned| a.free(owned);
    const keys = [_][]const u8{ "a", "b", "blocked" };
    {
        var db = try open(a, directory.path(), null);
        defer db.close();
        try db.setSchemaJson(a, before_json);
        try db.batch(.{ .timestamp_ns = 100, .writes = &.{
            .{ .key = "a", .value = "{\"id\":\"a\"}" },
            .{ .key = "b", .value = "{\"id\":\"b\"}" },
            .{ .key = "blocked", .value = "{\"id\":\"blocked\"}" },
        } });
        for (keys, &originals) |key, *saved| {
            saved.* = try physicalRow(a, &db, key);
            try expectPhysicalRow(a, &db, key, 1, saved.*);
        }
        inline for (.{ policies.Publication.Phase.pending_install, policies.Publication.Phase.serving_install }, 0..) |phase, i| {
            const bytes = try oldRowPolicyBundle(a, 1, phase, false);
            defer a.free(bytes);
            _ = try db.applyReplicatedRowPolicyPublication(bytes, try request(a, 1, phase), .{ .term = 3, .index = 11 + i });
            _ = try db.loadRowPolicyReceipt(1, phase);
        }
        inline for (.{ policies.Publication.Phase.pending_install, policies.Publication.Phase.serving_install }, 0..) |phase, i| {
            const bytes = try oldRowPolicyBundle(a, 2, phase, true);
            defer a.free(bytes);
            _ = try db.applyReplicatedRowPolicyPublication(bytes, try request(a, 2, phase), .{ .term = 3, .index = 20 + i });
            _ = try db.loadRowPolicyReceipt(2, phase);
        }
        try expectPair(a, &db, 2);
        try expectRow(a, &db, 2);
        try expectProtectedScan(a, &db, &.{ "a", "b" });
        for (keys, originals) |key, saved| try expectPhysicalRow(a, &db, key, 1, saved);
    }
    {
        var db = try open(a, directory.path(), null);
        defer db.close();
        try expectPair(a, &db, 2);
        try expectRow(a, &db, 2);
        try expectProtectedScan(a, &db, &.{ "a", "b" });
        for (keys, originals) |key, saved| try expectPhysicalRow(a, &db, key, 1, saved);
        const now: i64 = @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s));
        const ordinary_write_proof = try authority.sign(a, secret, issuer, .{ .principal = "alice", .roles = &.{"PUBLIC"}, .auth_revision = 1 }, .{ .table_id = 7, .table = "rows", .database = "main", .policy_generation = 2, .catalog_epoch = 3, .access = .write }, now);
        defer a.free(ordinary_write_proof);
        // SQL SELECT-sensitive row requirements require this distinct signed
        // producer purpose; an ordinary write token cannot assert them.
        const write_proof = try authority.sign(a, secret, issuer, .{ .principal = "alice", .roles = &.{"PUBLIC"}, .auth_revision = 1 }, .{ .table_id = 7, .table = "rows", .database = "main", .policy_generation = 2, .catalog_epoch = 3, .access = .write, .purpose = .sql_write }, now);
        defer a.free(write_proof);
        const read_proof = try token(a, 2);
        defer a.free(read_proof);
        var observed = (try db.lookup(a, "a", .{ .include_primary_digest = true, .row_policy_principal_proof = read_proof, .row_policy_database = "main" })) orelse return error.MissingCoupledRow;
        defer observed.deinit(a);
        const update_guard = [_]@import("types.zig").TransactionVersionPredicate{.{ .key = "a", .expected_version = observed.version orelse return error.MissingCoupledVersion, .expected_content_digest = observed.expected_content_digest orelse return error.MissingCoupledDigest, .row_policy_requirement = .update_select }};
        var update: @import("types.zig").BatchRequest = .{ .schema_version = 2, .relational_schema_version = 2, .timestamp_ns = 200, .predicates = &update_guard, .writes = &.{.{ .key = "a", .value = "{\"id\":\"blocked\"}" }}, .row_policy_principal_proof = write_proof, .row_policy_database = "main", .row_policy_admitted_at_seconds = now };
        update.row_policy_principal_proof = ordinary_write_proof;
        try std.testing.expectError(error.RowPolicyAuthenticationRequired, db.batch(update));
        try expectPhysicalRow(a, &db, "a", 1, originals[0]);
        update.row_policy_principal_proof = write_proof;
        try std.testing.expectError(error.RowPolicyDenied, db.batch(update));
        try expectPhysicalRow(a, &db, "a", 1, originals[0]);
        update.writes = &.{.{ .key = "a", .value = "{\"id\":\"a\"}" }};
        try db.batch(update);
        try expectPhysicalRow(a, &db, "a", 2, null);
        var deletion: @import("types.zig").BatchRequest = .{ .schema_version = 2, .relational_schema_version = 2, .timestamp_ns = 201, .predicates = &.{.{ .key = "blocked", .expected_version = 100, .row_policy_requirement = .delete_select }}, .deletes = &.{"blocked"}, .row_policy_principal_proof = write_proof, .row_policy_database = "main", .row_policy_admitted_at_seconds = now };
        try std.testing.expectError(error.RowPolicyDenied, db.batch(deletion));
        try expectPhysicalRow(a, &db, "blocked", 1, originals[2]);
        try expectPhysicalRow(a, &db, "b", 1, originals[1]);
        deletion.predicates = &.{.{ .key = "b", .expected_version = 100, .row_policy_requirement = .delete_select }};
        deletion.deletes = &.{"b"};
        try db.batch(deletion);
        try std.testing.expectError(error.NotFound, physicalRow(a, &db, "b"));
        try expectProtectedScan(a, &db, &.{"a"});
        try expectRow(a, &db, 2);
    }
    {
        var db = try open(a, directory.path(), null);
        defer db.close();
        try expectPhysicalRow(a, &db, "a", 2, null);
        try expectPhysicalRow(a, &db, "blocked", 1, originals[2]);
        try std.testing.expectError(error.NotFound, physicalRow(a, &db, "b"));
        try expectProtectedScan(a, &db, &.{"a"});
    }
}
