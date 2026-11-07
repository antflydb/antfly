// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Native metadata reducer qualification. Runtime feature advertisement and the
//! public TEMP campaign remain independently gated by their real capabilities.
const std = @import("std");
const local = @import("antfly_local_sources");
const module = @import("abandoned_temporary_retirement.zig");
const scopes = @import("scope_retirement_ledger.zig");
const table = @import("table_retirement_ledger.zig");
const locks = local.storage_pessimistic_lock_authority;
const authority = @import("../system_catalog/lock_authority.zig");
const sources = @import("../system_catalog/temporary_source_binding.zig");
const native = local.storage_pessimistic_lock_control;

// Any dependency on the current/recycled metadata Source is a test failure.
// The production proof must survive independent source-directory reclamation.
const ArchivedReader = struct {
    txn: *local.storage_docstore.DocStore.Txn,
    pub fn get(self: @This(), key: []const u8) ![]const u8 {
        if (std.mem.indexOf(u8, key, "sql_lock_scope:") != null or std.mem.indexOf(u8, key, "sql_lock_participant:") != null)
            return error.LiveSourceLookupForbidden;
        return self.txn.get(key);
    }
};

test "SQL scope retirement abandonment archive retains exact old participants through successor cold read and late ACK" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const gpa = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(gpa, ".zig-cache/tmp/{s}/abandoned-private-archive", .{tmp.sub_path}, 0);
    const incarnation: [16]u8 = @splat(1);
    const metadata_group: u64 = 21;
    const owner: local.common_topology_records.RelationPersistence.Owner = .{ .issuer_node_id = 7, .connection_id = @splat('a'), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 1, .principal_digest = @splat(4) };
    const descriptor: local.common_topology_records.RelationPersistence.Descriptor = .{ .kind = .temporary, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(5), .data_generation = 1, .owner = owner };
    const ns: locks.ledger.Namespace = .{ .home = .metadata, .group = metadata_group, .incarnation = incarnation };
    const registration: locks.Registration = .{ .group = metadata_group, .connection = owner.connection_id, .nonce = @splat(6), .standalone = false, .lease = .{ .backend = .{ .incarnation = incarnation, .id = @splat(7), .fence = 1 }, .database = 10, .node = owner.issuer_node_id, .principal = try authority.principalDigest(gpa, .bootstrap), .expires_at_ms = 1000 } };
    const command: locks.Command = .{ .sequence = 1, .nonce = @splat(8), .executor = owner.execution_instance };
    const route: local.metadata_catalog_route_contract.CatalogRouteFence = .{ .metadata_group_id = metadata_group, .metadata_incarnation = std.fmt.bytesToHex(incarnation, .lower), .catalog_revision = 1, .table_id = 100, .topology_epoch = 1, .route = .{ .group_id = 300, .range_id = 302, .identity_namespace = .{ .table_id = 100, .shard_id = 301, .range_id = 302 }, .execution = .{ .native_local = descriptor } }, .native_participant = .{ .physical_root_incarnation = std.math.maxInt(u128), .table_id = 100, .shard_id = 301, .range_id = 302, .data_generation = 1 } };
    const spec: locks.Data.Spec = .{ .table_name = "table:100", .route = route };
    var context: authority.Context = .{ .a = gpa, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = 10, .standalone = false, .enrollment = spec };
    const lookup: scopes.Lookup = .{ .group = metadata_group, .reference = .{ .id = @splat(9), .digest = @splat(10), .database = 10, .root = 20, .kind = .namespace, .tables = 1, .resources = 1 } };
    var parent: scopes.Head = .{ .lookup = lookup, .metadata_incarnation = incarnation, .creator = .bootstrap, .target_digest = @splat(11), .temporary_owner = owner, .sequence = 1, .progress = .{ .reference = lookup.reference } };
    const child: scopes.Child = .{ .lookup = lookup, .ordinal = 0, .table = route.table_id, .id = try lookup.reference.childId(route.table_id), .definition = @splat(12), .ranges = @splat(13) };
    const child_lookup: table.Lookup = .{ .group = metadata_group, .table = child.table, .id = child.id };
    var admitted: locks.Data.Participant = undefined;
    var before_successor: []const u8 = undefined;
    var closed: native.Snapshot = undefined;
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try locks.register(gpa, &txn, ns, registration, context);
        _ = try locks.begin(gpa, &txn, ns, registration.lease.backend, command, context);
        admitted = try locks.Data.enroll(gpa, &txn, ns, registration.lease.backend, command, @splat(14), spec, context);
        const native_registration = try admitted.admission.registration();
        const finished: native.Snapshot = .{ .scope = .{ .registration = native_registration, .next_command = 2, .completed = try admitted.admission.command(), .transaction = .{ .id = admitted.admission.transaction } }, .owner = .{ .lease = native_registration.lease } };
        context.data_proof = finished;
        try locks.Data.acknowledge(gpa, &txn, ns, registration.lease.backend, admitted.admission.generation, command, context);
        try locks.finish(gpa, &txn, ns, registration.lease.backend, command, context);
        var successor = command;
        successor.sequence += 1;
        successor.nonce = @splat(15);
        successor.executor = @splat(16);
        _ = try locks.begin(gpa, &txn, ns, registration.lease.backend, successor, context);
        const source = (try locks.read(gpa, &txn, ns, registration.lease.backend)).?;
        // Capture the actual directory body after FINISH acknowledged the
        // original native command; the earlier enroll return remains historical.
        admitted = (try locks.Data.readParticipant(gpa, &txn, ns, registration.lease.backend, admitted.admission.generation)).?;
        before_successor = try std.json.Stringify.valueAlloc(gpa, source, .{});
        const captured: sources.Abandonment = .{ .binding = .{ .namespace = .{ .database_id = 10, .namespace_id = 20, .owner = owner }, .group = metadata_group, .backend = registration.lease.backend, .registration_nonce = registration.nonce, .source_principal = registration.lease.principal, .standalone = false, .creation_command = command }, .source = source };
        try std.testing.expect(try captured.eligible());
        var key: [512]u8 = undefined;
        const parent_record = try scopes.HeadRecord.init(parent);
        try txn.put(try scopes.key(&key, lookup, .head), parent_record.slice());
        parent = try module.stage(gpa, &txn, parent, captured);
        parent.progress = try parent.progress.drained();
        const drained_parent = try scopes.HeadRecord.init(parent);
        try txn.put(try scopes.key(&key, lookup, .head), drained_parent.slice());
        const child_record = try scopes.ChildRecord.init(child);
        try txn.put(try scopes.childKey(&key, lookup, child.ordinal), child_record.slice());
        var catalog_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash("", &catalog_digest, .{});
        const image: local.common_topology_records.TableRecord = .{ .table_id = route.table_id, .name = spec.table_name, .sql_persistence = descriptor };
        const ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = route.table_id, .group_id = route.route.group_id, .range_id = route.route.range_id, .doc_identity_shard_id = 301, .start_key = "" }};
        const fences = [_]local.storage_db_relational_integrity_topology_contract.Fence{.{ .transition_id = std.mem.readInt(u64, child.id[0..8], .little), .attempt = std.mem.readInt(u64, child.id[8..16], .little), .peer_group_id = 300, .owner_group_id = 300, .role = .document_retirement_source, .namespace = admitted.admission.binding.identity, .catalog_digest = catalog_digest }};
        const effects: @import("table_retirement_manifest.zig").Reference = .{ .digest = @splat(17), .root_table = route.table_id, .survivors = 0, .counters = 0, .views = 0 };
        const plan: @import("fk_generation_publication.zig").Plan = .{ .id = child.id, .child_before = image, .child_after = image, .child_catalog_before_b64 = "", .child_ranges = &ranges, .child_fences = &fences, .parents = &.{}, .table_retirement = effects, .sql_drop = .{ .target = .{ .database = "tenant", .namespace = "private", .name = "items" }, .logical_table = "items", .incarnation = incarnation, .owner = .{ .table_id = route.table_id, .schema_version = 0, .schema_digest = catalog_digest, .kind = .table } } };
        const physical = try table.physical.prepare(gpa, gpa, plan);
        const head: table.Head = .{ .lookup = child_lookup, .incarnation = std.fmt.bytesToHex(incarnation, .lower), .effects = effects, .scope = .{ .database = 10, .namespace = 20, .creator = .bootstrap, .parent = child }, .participant_count = 1, .range_count = 1, .participants_digest = @splat(18), .revision = 2, .phase = .retiring, .physical = .{ .summary = physical.summary } };
        const head_record = try table.HeadRecord.init(head);
        try txn.put(try table.key(&key, child_lookup, .head), head_record.slice());
        try txn.put(try table.physical.key(&key, child_lookup, .plan, 0, .source_fenced), physical.encoded);
        closed = .{ .scope = .{ .registration = native_registration, .next_command = 2, .completed = try admitted.admission.command(), .released_transaction = .{ .id = admitted.admission.transaction }, .state = .retired }, .owner = .{ .lease = native_registration.lease, .state = .retired } };
        try txn.commit();
    }
    // The exact source/body cut remains provable after cold open, independently
    // of current connection lookup or the original participant directory.
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const archived = ArchivedReader{ .txn = &txn };
        const selected = (try module.readWork(gpa, archived, .{ .child = child_lookup })).?;
        try std.testing.expectEqualDeep(admitted, selected.control.participant);
        var invalid = closed;
        invalid.scope.?.registration.nonce[0] ^= 1;
        try std.testing.expectError(error.LockOwnerChanged, module.acknowledge(gpa, &txn, .{ .child = child_lookup, .generation = admitted.admission.generation }, invalid));
        invalid = closed;
        invalid.owner.?.state = .active;
        try std.testing.expectError(error.LockOwnerBusy, module.acknowledge(gpa, &txn, .{ .child = child_lookup, .generation = admitted.admission.generation }, invalid));
        try module.acknowledge(gpa, &txn, .{ .child = child_lookup, .generation = admitted.admission.generation }, closed);
        // Lost reply cannot re-close or clear the current successor backend.
        try module.acknowledge(gpa, &txn, .{ .child = child_lookup, .generation = admitted.admission.generation }, closed);
        try std.testing.expect(try module.readWork(gpa, archived, .{ .child = child_lookup }) == null);
        try std.testing.expect((try module.readWork(gpa, archived, .{ .child = child_lookup, .generation = admitted.admission.generation })).?.completed);
        try std.testing.expectEqualStrings(before_successor, try std.json.Stringify.valueAlloc(gpa, (try locks.read(gpa, &txn, ns, registration.lease.backend)).?, .{}));
        try module.verifyHead(gpa, archived, parent);
        try txn.commit();
    }
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        var txn = try store.beginReadTxn();
        defer txn.abort();
        const archived = ArchivedReader{ .txn = &txn };
        try std.testing.expect((try module.readWork(gpa, archived, .{ .child = child_lookup, .generation = admitted.admission.generation })).?.completed);
    }
}
