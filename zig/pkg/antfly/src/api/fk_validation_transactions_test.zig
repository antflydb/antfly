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

//! Native transaction qualification for historical FK validation.
const std = @import("std");
const local = @import("antfly_local_sources");
const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
const planner = local.api_relational_integrity_commit;
const reads = local.api_table_read_source;
const types = local.storage_db_types;
const integrity = local.storage_db_relational_integrity_contract;
const catalog = local.storage_db_relational_integrity_catalog;
const activation = local.storage_db_relational_integrity_activation_contract;
const tuples = local.storage_db_relational_index_keys;
const a = std.testing.allocator;

const declaration =
    \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"fk","child_columns":["parent_id"],"parent_table":"rows","parent_columns":["id"],"not_valid":true}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true},"payload":{"type":"string"}},"additionalProperties":false}}}}
;
const tables = [_]local.common_topology_records.TableRecord{.{ .table_id = 300, .name = "rows", .schema_json = declaration }};
const ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = 300, .group_id = 301, .start_key = "" }};

const Fixture = struct {
    db: *db_mod.DB,
    next_transaction: u8 = 20,
    fn lookup(ptr: *anyopaque, backing: std.mem.Allocator, table: []const u8, key: []const u8, options: types.LookupOptions, consistency: local.storage_read_consistency.ReadConsistency) !?reads.LookupResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqualStrings("rows", table);
        try std.testing.expectEqual(local.storage_read_consistency.ReadConsistency.read_index, consistency);
        const value = (try self.db.lookup(backing, key, options)) orelse return null;
        return .{ .json = value.json, .version = value.version orelse try self.db.getTimestamp(backing, key), .expected_content_digest = value.expected_content_digest };
    }
    fn source(self: *@This()) reads.TableReadSource {
        return .{ .ptr = self, .vtable = &.{ .lookup = lookup, .scan = undefined, .query = undefined } };
    }
    fn write(ptr: *anyopaque, backing: std.mem.Allocator, requests: []const local.api_distributed_txn_contract.TableCommitRequest, _: types.SyncLevel, cancellation: types.CancellationToken) !?local.api_distributed_txn_contract.CommitOutcome {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try cancellation.check();
        var borrowed: planner.Prepared = .{ .arena = std.heap.ArenaAllocator.init(backing), .tables = requests };
        defer borrowed.deinit();
        const id = self.next_transaction;
        self.next_transaction += 1;
        try self.commit(&borrowed, id);
        return .{ .committed = .{ .participant_count = 1 } };
    }
    fn writer(self: *@This()) local.api_table_write_source.TableWriteSource {
        return .{ .ptr = self, .vtable = &.{ .batch = undefined, .commit_batch_with_cancellation = write } };
    }
    fn page(self: *@This(), key: []const u8) !planner.Prepared {
        var row = (try self.db.lookup(a, key, .{ .include_primary_digest = true })) orelse return error.NotFound;
        defer row.deinit(a);
        return planner.prepareForeignKeyValidationPage(a, self.source(), &tables, &ranges, "rows", &.{.{ .key = key, .json = row.json, .version = row.version orelse try self.db.getTimestamp(a, key), .expected_content_digest = row.expected_content_digest }}, &.{"fk"}, .{});
    }
    fn prepare(self: *@This(), prepared: *const planner.Prepared, id: u8) !types.TxnId {
        try std.testing.expectEqual(@as(usize, 1), prepared.tables.len);
        const request = prepared.tables[0];
        const txn = try self.db.beginTransactionWithId(@splat(id), @as(u64, id) * 100);
        errdefer self.db.abortTransaction(txn, @as(u64, id) * 100 + 1) catch {};
        // Exercise the production participant envelope/parser before native
        // preparation, including exact numeric roots and binary progress.
        const transport = @import("distributed_txn.zig");
        const wire = try transport.encodeTxnPrepareRequest(a, .{ .txn_id = txn, .req = .{
            .relational_schema_version = request.relational_schema_version,
            .relational_integrity_generation_set = request.relational_integrity_generation_set,
            .writes = request.writes,
            .deletes = request.deletes,
            .predicates = request.predicates,
            .integrity = request.integrity,
            .integrity_commands = request.integrity_commands,
            .relational_activation = request.relational_activation,
        } });
        defer a.free(wire);
        var decoded = try transport.parseTxnPrepareRequest(a, wire);
        defer transport.freeTxnPrepareRequest(a, &decoded);
        try std.testing.expectEqualDeep(txn, decoded.txn_id);
        try self.db.writeTransaction(txn, decoded.req);
        return txn;
    }
    fn commit(self: *@This(), prepared: *const planner.Prepared, id: u8) !void {
        const txn = try self.prepare(prepared, id);
        try self.db.commitTransaction(txn, @as(u64, id) * 100 + 1);
    }
};

fn checkpoint(prepared: *planner.Prepared, command: activation.Command) !void {
    const requests = try prepared.arena.allocator().dupe(local.api_distributed_txn_contract.TableCommitRequest, prepared.tables);
    prepared.tables = requests;
    requests[0].relational_activation = command;
}

fn expectCheckpoint(db: *db_mod.DB, expected: []const u8) !void {
    const raw = (try db.core.getStoreValue(a, activation.validation_key)).?;
    defer a.free(raw);
    try std.testing.expectEqualSlices(u8, expected, raw);
}

fn expectHistorical(db: *db_mod.DB, key: []const u8, wanted: bool) !void {
    const raw = (try db.core.getStoreValue(a, key)) orelse return error.NotFound;
    defer a.free(raw);
    try std.testing.expectEqual(wanted, (try integrity.Reference.decode(key, raw)).historical_unvalidated);
}

test "foreign key validation native transaction preserves atomic pages and publication across cold recovery" {
    var directory = try local.common_test_directory.TestDirectory.init("fk-validation-transactions");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 300, .shard_id = 301, .range_id = 302 }, .online_source_authority = .native, .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    var db = try db_mod.DB.open(a, directory.path(), options);
    var live = true;
    defer if (live) db.close();
    try db.setSchemaJson(a, declaration);
    const raw_catalog = (try db.core.getStoreValue(a, catalog.key)).?;
    defer a.free(raw_catalog);
    var bindings = try catalog.decode(a, raw_catalog);
    defer bindings.deinit();
    const generation_set = activation.generationSet(bindings);
    var view = db.core.acquireSchemaView().?;
    defer view.release();
    var tuple = try tuples.TuplePlan.init(a, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
    defer tuple.deinit();
    var tuple_bytes: std.ArrayList(u8) = .empty;
    defer tuple_bytes.deinit(a);
    var addresses: [4]integrity.Address = undefined;
    var encoded_tuples: [4][]u8 = undefined;
    var initialized: usize = 0;
    defer for (encoded_tuples[0..initialized]) |bytes| a.free(bytes);
    for ([_]i64{ 1, 2, 3, 99 }, &addresses, &encoded_tuples) |id, *address, *encoded| {
        tuple_bytes.clearRetainingCapacity();
        _ = try tuple.appendValues(a, &tuple_bytes, &.{.{ .integer = id }});
        encoded.* = try a.dupe(u8, tuple_bytes.items);
        initialized += 1;
        address.* = try integrity.Address.init(bindings.find(.unique, "pk").?.generation, encoded.*);
    }
    var establishes: [3]integrity.Command = undefined;
    for ([_][]const u8{ "parent", "child", "orphan" }, &establishes, 0..) |key, *command, i| command.* = .{ .address = addresses[i], .operation = .{ .establish = .{ .tuple = encoded_tuples[i], .parent_table = "rows", .parent_key = key, .schema_version = 1 } } };
    // Native receiver fixture: seed a historical image with explicit current
    // schema/generation admission. The SQL campaign separately qualifies DDL.
    const seeded = try db.beginTransactionWithId(@splat(1), 100);
    try db.writeTransaction(seeded, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generation_set, .writes = &.{
        .{ .key = "parent", .value = "{\"id\":1,\"parent_id\":null,\"payload\":\"root\"}" },
        .{ .key = "child", .value = "{\"id\":2,\"parent_id\":1,\"payload\":\"before\"}" },
        .{ .key = "orphan", .value = "{\"id\":3,\"parent_id\":99,\"payload\":\"orphan\"}" },
    }, .integrity_commands = &establishes });
    try db.commitTransaction(seeded, 101);
    const child_ref: integrity.Reference = .{ .child_table = "rows", .child_key = "child", .constraint_name = "fk", .constraint_generation = bindings.find(.foreign_key, "fk").?.generation, .historical_unvalidated = true };
    var orphan_ref = child_ref;
    orphan_ref.child_key = "orphan";
    const child_key = try child_ref.key(addresses[0]);
    const orphan_key = try orphan_ref.key(addresses[3]);
    const child_bytes = try child_ref.encode(a, addresses[0]);
    defer a.free(child_bytes);
    const orphan_bytes = try orphan_ref.encode(a, addresses[3]);
    defer a.free(orphan_bytes);
    try db.core.store.put(&child_key, child_bytes);
    try db.core.store.put(&orphan_key, orphan_bytes);
    var fixture: Fixture = .{ .db = &db };
    const native_before = try local.storage_schema.serializeSchema(a, view.tableSchema().*);
    defer a.free(native_before);
    const versioned = try std.mem.replaceOwned(u8, a, declaration, "\"version\":1", "\"version\":2");
    defer a.free(versioned);
    const after_schema = try std.mem.replaceOwned(u8, a, versioned, "\"not_valid\":true", "\"not_valid\":false");
    defer a.free(after_schema);
    const job = local.storage_db_relational_nullability_job;
    const hash = local.storage_db_relational_schema_owner_contract.hash;
    var candidate = try job.Candidate.initForeignKeySource(a, 300, declaration, after_schema, raw_catalog, .{ .table_name = "rows", .parents = &.{.{ .constraint_name = "fk", .declared_parent = "rows", .parent_table = "rows", .parent_table_id = 300, .parent_schema_version = 1, .parent_schema = hash(native_before), .parent_public = hash(declaration), .parent_catalog = hash(raw_catalog), .parent_unique = "pk", .parent_generation = bindings.find(.unique, "pk").?.generation }} }, .{});
    defer candidate.deinit();
    const identity = try db.relationalTopologyIdentity();
    var source_read = try db.core.store.beginReadTxn();
    const source_binding = job.prepareBinding(&source_read, &candidate, @splat(42), @splat(43), .{ .namespace = options.identity_namespace.?, .owner_group_id = 301, .peer_group_id = 301, .transition_id = 42, .attempt = 1, .admission_epoch = identity.next_epoch, .catalog_digest = identity.catalog_digest, .role = .schema_validation }, .native) catch |err| {
        source_read.abort();
        return err;
    };
    source_read.abort();
    const record: activation.ForeignKeyValidationRecord = .{ .binding = source_binding, .physical_root = db.root_incarnation, .revision = 1, .progress = .{ .generation_set = generation_set, .owner = source_binding.owner, .schema_version = 1, .phase = .foreign_key } };
    const initial_record = try record.encode(a);
    defer a.free(initial_record);
    const scope: activation.ForeignKeyValidationScope = .{ .binding = source_binding, .physical_root = db.root_incarnation, .action = .page };
    const begin = try db.beginTransactionWithId(@splat(8), 800);
    try db.writeTransaction(begin, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generation_set, .relational_activation = .{ .routing_key = "", .expected = null, .next = initial_record, .validation = .{ .binding = source_binding, .physical_root = db.root_incarnation, .action = .begin, .inputs = .{ .bytes = candidate.inputs } } } });
    try db.commitTransaction(begin, 801);
    try expectCheckpoint(&db, initial_record);
    // Root mismatches fail before any validation intent can be accepted.
    const wrong_root_txn = try db.beginTransactionWithId(@splat(12), 1200);
    var wrong_scope = scope;
    wrong_scope.physical_root ^= 1;
    if (wrong_scope.physical_root == 0) wrong_scope.physical_root = 2;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, db.writeTransaction(wrong_root_txn, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generation_set, .relational_activation = .{ .routing_key = "", .expected = initial_record, .next = initial_record, .validation = wrong_scope } }));
    try db.abortTransaction(wrong_root_txn, 1201);
    try expectCheckpoint(&db, initial_record);
    try std.testing.expectEqual(source_binding.fence.admission_epoch + 1, (try db.relationalTopologyIdentity()).next_epoch);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.IntegrityTopologyBusy, local.storage_db_relational_integrity_topology.requireUnfenced(&read));
        try local.storage_db_relational_integrity_topology.requireWriteUnfenced(&read);
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, job.validateSnapshot(a, &read, options.identity_namespace.?, .{ .io = std.Options.debug_io, .directory = directory.path() }));
    }
    const cursor = try local.storage_internal_keys.relationalRowKeyAlloc(a, "child");
    defer a.free(cursor);
    var next = record;
    next.revision += 1;
    next.progress.rows_scanned = 1;
    next.progress.cursor = cursor;
    const next_record = try next.encode(a);
    defer a.free(next_record);
    const page_command: activation.Command = .{ .routing_key = "", .expected = initial_record, .next = next_record, .validation = scope };
    var absent = try fixture.page("orphan");
    defer absent.deinit();
    try checkpoint(&absent, page_command);
    try std.testing.expectError(error.ForeignKeyParentMissing, fixture.prepare(&absent, 2));
    try expectHistorical(&db, &orphan_key, true);
    try expectCheckpoint(&db, initial_record);
    var stale = try fixture.page("child");
    defer stale.deinit();
    try checkpoint(&stale, page_command);
    const changed = try db.beginTransactionWithId(@splat(3), 300);
    try db.writeTransaction(changed, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generation_set, .writes = &.{.{ .key = "child", .value = "{\"id\":2,\"parent_id\":1,\"payload\":\"changed\"}" }} });
    try db.commitTransaction(changed, 301);
    try std.testing.expectError(error.VersionConflict, fixture.prepare(&stale, 4));
    try expectHistorical(&db, &child_key, true);
    try expectCheckpoint(&db, initial_record);
    var checked = try fixture.page("child");
    defer checked.deinit();
    try checkpoint(&checked, page_command);
    const before = (try db.lookup(a, "child", .{ .include_primary_digest = true })).?;
    defer a.free(before.json);
    const coverage = (try db.core.getStoreValue(a, activation.key)).?;
    defer a.free(coverage);
    const promoted = try fixture.prepare(&checked, 5);
    try expectHistorical(&db, &child_key, true);
    try expectCheckpoint(&db, initial_record);
    db.close();
    live = false;
    db = try db_mod.DB.open(a, directory.path(), options);
    live = true;
    try db.commitTransaction(promoted, 501);
    try expectHistorical(&db, &child_key, false);
    try expectCheckpoint(&db, next_record);
    // A successfully committed predecessor cannot be reused after reopen.
    try std.testing.expectError(error.ConstraintActivationChanged, fixture.prepare(&checked, 13));
    try expectCheckpoint(&db, next_record);
    var after = (try db.lookup(a, "child", .{ .include_primary_digest = true })).?;
    defer after.deinit(a);
    try std.testing.expectEqual(before.version, after.version);
    try std.testing.expectEqual(before.expected_content_digest, after.expected_content_digest);
    const retained_catalog = (try db.core.getStoreValue(a, catalog.key)).?;
    defer a.free(retained_catalog);
    try std.testing.expectEqualSlices(u8, raw_catalog, retained_catalog);
    const retained_coverage = (try db.core.getStoreValue(a, activation.key)).?;
    defer a.free(retained_coverage);
    try std.testing.expectEqualSlices(u8, coverage, retained_coverage);
    // Unchanged NOT VALID updates cannot weaken an already validated edge.
    var update = try planner.prepareWithCoverageControlled(a, fixture.source(), &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "child", .value = "{\"id\":2,\"parent_id\":1,\"payload\":\"after\"}" }} }}, .{});
    defer update.deinit();
    try fixture.commit(&update, 6);
    try expectHistorical(&db, &child_key, false);
    const release = try db.beginTransactionWithId(@splat(7), 700);
    try std.testing.expectError(error.ForeignKeyReferenced, db.writeTransaction(release, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generation_set, .integrity_commands = &.{.{ .address = addresses[0], .operation = .{ .release = .{ .parent_table = "rows", .parent_key = "parent" } } }} }));
    try db.abortTransaction(release, 701);
    // The real bounded worker discovers the remaining orphan through native
    // projected pages. Its absence guard and diagnostic commit together and
    // retain the successful prefix, without clearing the public declaration.
    const worker = local.api_relational_activation_worker;
    try std.testing.expect(try worker.runForeignKeyValidationPage(a, fixture.source(), fixture.writer(), &tables, &ranges, ranges[0], source_binding.id));
    const failed_raw = (try db.core.getStoreValue(a, activation.validation_key)).?;
    defer a.free(failed_raw);
    const failed = try activation.ForeignKeyValidationRecord.decode(failed_raw);
    try std.testing.expectEqual(activation.State.invalid, failed.progress.state);
    try std.testing.expectEqualStrings("ForeignKeyParentMissing", failed.progress.failure);
    try std.testing.expectEqual(@as(u64, 1), failed.progress.rows_scanned);
    try std.testing.expectEqualSlices(u8, cursor, failed.progress.cursor);
    var repair = try planner.prepareWithCoverageControlled(a, fixture.source(), &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "repaired_parent", .value = "{\"id\":99,\"parent_id\":null,\"payload\":\"repair\"}" }} }}, .{});
    defer repair.deinit();
    try fixture.commit(&repair, 9);
    const retry_identity = try db.relationalTopologyIdentity();
    var retry_read = try db.core.store.beginReadTxn();
    const retry_binding = job.prepareBinding(&retry_read, &candidate, @splat(44), @splat(45), .{ .namespace = options.identity_namespace.?, .owner_group_id = 301, .peer_group_id = 301, .transition_id = 44, .attempt = 1, .admission_epoch = retry_identity.next_epoch, .catalog_digest = retry_identity.catalog_digest, .role = .schema_validation }, .native) catch |err| {
        retry_read.abort();
        return err;
    };
    retry_read.abort();
    const retry_record: activation.ForeignKeyValidationRecord = .{ .binding = retry_binding, .physical_root = db.root_incarnation, .revision = 1, .progress = .{ .generation_set = generation_set, .owner = retry_binding.owner, .schema_version = 1, .phase = .foreign_key } };
    const retry_initial = try retry_record.encode(a);
    defer a.free(retry_initial);
    const retry_begin = try db.beginTransactionWithId(@splat(10), 1000);
    try db.writeTransaction(retry_begin, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generation_set, .relational_activation = .{ .routing_key = "", .expected = failed_raw, .next = retry_initial, .validation = .{ .binding = retry_binding, .physical_root = db.root_incarnation, .action = .begin, .inputs = .{ .bytes = candidate.inputs } } } });
    try db.commitTransaction(retry_begin, 1001);
    for (0..16) |_| {
        if (!try worker.runForeignKeyValidationPage(a, fixture.source(), fixture.writer(), &tables, &ranges, ranges[0], retry_binding.id)) break;
    } else return error.ValidationDidNotConverge;
    const ready_raw = (try db.core.getStoreValue(a, activation.validation_key)).?;
    defer a.free(ready_raw);
    const ready = try activation.ForeignKeyValidationRecord.decode(ready_raw);
    try std.testing.expectEqual(activation.State.enforced, ready.progress.state);
    try std.testing.expectEqual(@as(u64, 4), ready.progress.rows_scanned);
    try expectHistorical(&db, &child_key, false);
    try expectHistorical(&db, &orphan_key, false);
    const public_after = (try db.core.getStoreValue(a, "\x00\x00__metadata__:schema_json")).?;
    defer a.free(public_after);
    try std.testing.expectEqualStrings(declaration, public_after);
    var canceled = ready;
    canceled.revision += 1;
    canceled.progress.state = .invalid;
    canceled.progress.failure = "Canceled";
    const canceled_raw = try canceled.encode(a);
    defer a.free(canceled_raw);
    const cancel = try db.beginTransactionWithId(@splat(11), 1100);
    try db.writeTransaction(cancel, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generation_set, .relational_activation = .{ .routing_key = "", .expected = ready_raw, .next = canceled_raw, .validation = .{ .binding = retry_binding, .physical_root = db.root_incarnation, .action = .cancel } } });
    try db.commitTransaction(cancel, 1101);
    {
        var released = try db.core.store.beginReadTxn();
        defer released.abort();
        try local.storage_db_relational_integrity_topology.requireUnfenced(&released);
    }
    // A second complete validation exercises the real schema publication
    // participant, not a direct public-schema KV substitution.
    const install_identity = try db.relationalTopologyIdentity();
    var install_read = try db.core.store.beginReadTxn();
    const install_binding = job.prepareBinding(&install_read, &candidate, @splat(46), @splat(47), .{ .namespace = options.identity_namespace.?, .owner_group_id = 301, .peer_group_id = 301, .transition_id = 46, .attempt = 1, .admission_epoch = install_identity.next_epoch, .catalog_digest = install_identity.catalog_digest, .role = .schema_validation }, .native) catch |err| {
        install_read.abort();
        return err;
    };
    install_read.abort();
    const install_record: activation.ForeignKeyValidationRecord = .{ .binding = install_binding, .physical_root = db.root_incarnation, .revision = 1, .progress = .{ .generation_set = generation_set, .owner = install_binding.owner, .schema_version = 1, .phase = .foreign_key } };
    const install_initial = try install_record.encode(a);
    defer a.free(install_initial);
    const install_begin = try db.beginTransactionWithId(@splat(14), 1400);
    try db.writeTransaction(install_begin, .{ .relational_schema_version = 1, .relational_integrity_generation_set = generation_set, .relational_activation = .{ .routing_key = "", .expected = canceled_raw, .next = install_initial, .validation = .{ .binding = install_binding, .physical_root = db.root_incarnation, .action = .begin, .inputs = .{ .bytes = candidate.inputs } } } });
    try db.commitTransaction(install_begin, 1401);
    try std.testing.expectError(error.SchemaValidationNotReady, applyControl(&db, .{ .binding = install_binding, .action = .begin, .payload = candidate.inputs }));
    for (0..16) |_| {
        if (!try worker.runForeignKeyValidationPage(a, fixture.source(), fixture.writer(), &tables, &ranges, ranges[0], install_binding.id)) break;
    } else return error.ValidationDidNotConverge;
    const original_row_key = try local.storage_internal_keys.relationalRowKeyAlloc(a, "child");
    defer a.free(original_row_key);
    const original_row = try db.core.store.get(a, original_row_key);
    defer a.free(original_row);
    var substituted = install_binding;
    substituted.id[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, applyControl(&db, .{ .binding = substituted, .action = .begin, .payload = candidate.inputs }));
    const completed_raw = (try db.core.getStoreValue(a, activation.validation_key)).?;
    defer a.free(completed_raw);
    const completed_record = try activation.ForeignKeyValidationRecord.decode(completed_raw);
    {
        // Match the budgeted ABI producer, which prepares directly and enters
        // through the DB's common apply boundary rather than its convenience
        // preparation wrapper. A copied record must still fail closed.
        var direct = try db_mod.DB.NativeSchemaOwnerControl.Prepared.init(a, db.core, .{ .binding = install_binding, .action = .begin, .payload = candidate.inputs }, .{});
        defer direct.deinit();
        var copied = completed_record;
        copied.physical_root ^= 1;
        if (copied.physical_root == 0) copied.physical_root = 2;
        const copied_raw = try copied.encode(a);
        defer a.free(copied_raw);
        try db.core.store.put(activation.validation_key, copied_raw);
        defer db.core.store.put(activation.validation_key, completed_raw) catch unreachable;
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, db.applyPreparedNativeSchemaOwnerControl(&direct, null));
    }
    const admitted = try applyControl(&db, .{ .binding = install_binding, .action = .begin, .payload = candidate.inputs });
    try std.testing.expectEqual(job.Phase.fencing, admitted.phase);
    {
        var substituted_record = completed_record;
        substituted_record.revision += 1;
        const substituted_raw = try substituted_record.encode(a);
        defer a.free(substituted_raw);
        try db.core.store.put(activation.validation_key, substituted_raw);
        defer db.core.store.put(activation.validation_key, completed_raw) catch unreachable;
        try std.testing.expectError(error.SchemaValidationJobChanged, applyControl(&db, .{ .binding = install_binding, .action = .freeze }));
    }
    const frozen = try applyControl(&db, .{ .binding = install_binding, .action = .freeze });
    try std.testing.expectEqual(job.Phase.ready, frozen.phase);
    try std.testing.expect(frozen.reference_coverage == null);
    try std.testing.expect(frozen.transaction_coverage != null);
    try std.testing.expectEqual(@as(u64, 4), frozen.rows_scanned);
    try std.testing.expectEqualDeep(frozen, try applyControl(&db, .{ .binding = install_binding, .action = .freeze }));
    var install_payload: [64]u8 = undefined;
    @memset(install_payload[0..32], 91);
    @memcpy(install_payload[32..64], &try frozen.digest());
    const install_command: db_mod.DB.NativeSchemaOwnerControl.Command = .{ .binding = install_binding, .action = .install, .payload = &install_payload };
    const Fault = struct {
        pub fn validateBefore(_: @This(), _: anytype) !void {}
        pub fn stage(_: @This(), txn: anytype, _: job.Header) !void {
            try txn.put("\x00\x00__metadata__:fk_validation_fault_outbox", "must roll back");
            return error.InjectedValidationInstallFailure;
        }
    };
    {
        var prepared = try db.prepareNativeSchemaOwnerControl(install_command, .{});
        defer prepared.deinit();
        db.core.lockApply();
        defer db.core.unlockApply();
        try std.testing.expectError(error.InjectedValidationInstallFailure, prepared.commitSchema(db.core, null, Fault{}));
    }
    try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
    try std.testing.expect((try db.core.getStoreValue(a, "\x00\x00__metadata__:fk_validation_fault_outbox")) == null);
    const installed = try applyControl(&db, install_command);
    try std.testing.expectEqual(job.Phase.installed, installed.phase);
    try std.testing.expectEqual(@as(u32, 2), db.core.schema.?.version);
    try std.testing.expectError(error.SchemaValidationPublicationRequired, applyControl(&db, .{ .binding = install_binding, .action = .cancel }));
    const successor = (try db.core.getStoreValue(a, catalog.key)).?;
    defer a.free(successor);
    try std.testing.expectEqualSlices(u8, candidate.catalog_after, successor);
    var successor_catalog = try catalog.decode(a, successor);
    defer successor_catalog.deinit();
    for (bindings.bindings) |binding| {
        try std.testing.expectEqualDeep(binding.generation, successor_catalog.find(binding.definition.kind, binding.definition.name).?.generation);
    }
    db.close();
    live = false;
    db = try db_mod.DB.open(a, directory.path(), options);
    live = true;
    try std.testing.expectEqualDeep(installed, (try db.observeNativeSchemaOwner(install_binding, .{})).header.?);
    const duplicate_install = try applyControl(&db, install_command);
    try std.testing.expectEqualDeep(installed, duplicate_install);
    const current_row = try db.core.store.get(a, original_row_key);
    defer a.free(current_row);
    try std.testing.expectEqualSlices(u8, original_row, current_row);
    var release_payload: [96]u8 = undefined;
    @memcpy(release_payload[0..32], install_payload[0..32]);
    @memcpy(release_payload[32..64], &try installed.digest());
    @memset(release_payload[64..96], 92);
    const published = try applyControl(&db, .{ .binding = install_binding, .action = .release, .payload = &release_payload });
    try std.testing.expectEqual(job.Phase.released, published.phase);
    try std.testing.expect((try db.core.getStoreValue(a, activation.validation_key)) == null);
    try std.testing.expect((try db.core.getStoreValue(a, activation.validation_inputs_key)) == null);
    var finished = try db.core.store.beginReadTxn();
    defer finished.abort();
    try local.storage_db_relational_integrity_topology.requireUnfenced(&finished);
    const final_coverage = try activation.Progress.decode(try finished.get(activation.key));
    try std.testing.expectEqual(activation.State.enforced, final_coverage.state);
    try std.testing.expect(final_coverage.matches(successor_catalog, install_binding.owner));
    try std.testing.expectEqual(@as(u32, 2), final_coverage.schema_version);
    const successor_public = (try db.core.getStoreValue(a, "\x00\x00__metadata__:schema_json")).?;
    defer a.free(successor_public);
    try std.testing.expectEqualStrings(after_schema, successor_public);
    const next_tables = [_]local.common_topology_records.TableRecord{.{ .table_id = 300, .name = "rows", .schema_json = after_schema }};
    // Same-owner parent claims are checked atomically by the participant,
    // rather than by a racy coordinator-side existence read.
    var rejected_child = try planner.prepareWithCoverageControlled(a, fixture.source(), &next_tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "new_orphan", .value = "{\"id\":4,\"parent_id\":999,\"payload\":\"bad\"}" }} }}, .{});
    defer rejected_child.deinit();
    try std.testing.expectError(error.ForeignKeyParentMissing, fixture.prepare(&rejected_child, 16));
    var accepted = try planner.prepareWithCoverageControlled(a, fixture.source(), &next_tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "new_child", .value = "{\"id\":4,\"parent_id\":1,\"payload\":\"good\"}" }} }}, .{});
    defer accepted.deinit();
    try fixture.commit(&accepted, 15);
    parent_delete: {
        var rejected_parent = planner.prepareWithCoverageControlled(a, fixture.source(), &next_tables, &ranges, &.{.{ .table_name = "rows", .deletes = &.{"parent"} }}, .{}) catch |err| {
            try std.testing.expectEqual(error.ForeignKeyReferenced, err);
            break :parent_delete;
        };
        defer rejected_parent.deinit();
        try std.testing.expectError(error.ForeignKeyReferenced, fixture.prepare(&rejected_parent, 17));
    }
}

fn applyControl(db: *db_mod.DB, command: db_mod.DB.NativeSchemaOwnerControl.Command) !local.storage_db_relational_nullability_job.Header {
    var prepared = try db.prepareNativeSchemaOwnerControl(command, .{});
    defer prepared.deinit();
    return (try db.applyPreparedNativeSchemaOwnerControl(&prepared, null)).header.?;
}
