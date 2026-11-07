// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const admission = @import("trigger_native_admission.zig");
const handoff = @import("trigger_handoff.zig");
const manifests = @import("trigger_manifest.zig");
const attachments = @import("../system_catalog/triggers.zig");
const records = @import("../system_catalog/routines.zig");
const authority = @import("../common/trigger_preparation_authority.zig");
const types = @import("../storage/db/types.zig");
const batch = @import("../api/batch.zig");

const View = struct {
    state: attachments.State,
    schema_digest: [32]u8 = @splat(2),
    pub fn triggerState(self: *const @This()) ?*const attachments.State {
        return &self.state;
    }
    pub fn version(_: @This()) u32 {
        return 3;
    }
    pub fn validator(self: @This()) ?struct { source_digest: ?[32]u8 } {
        return .{ .source_digest = self.schema_digest };
    }
};
const Signer = struct {
    fn sign(_: *anyopaque, a: std.mem.Allocator, scope: authority.Scope) ![]u8 {
        return authority.sign(a, "actual-user-authority-key", "issuer", scope, 100);
    }
};
fn receipt(a: std.mem.Allocator) !handoff.Receipt {
    const reference: records.RoutineReference = .{ .database = "antfly", .namespace = "public", .name = "touch_updated_at", .namespace_id = 2, .id = 9 };
    const state: attachments.State = .{ .metadata_incarnation = @splat(1), .table_id = 7, .generation = 1, .next_id = 2, .entries = try a.dupe(attachments.Entry, &.{.{ .id = 1, .generation = 1, .name = "update_timestamp", .timing = .before, .events = .{ .update = true }, .level = .statement, .function = reference, .arguments = &.{"updated_at_ns"} }}) };
    const program: manifests.Manifest = .{ .metadata_incarnation = @splat(1), .catalog_revision = 1, .table = .{ .id = 7, .schema_version = 3, .schema_digest = @splat(2), .attachment_generation = 1, .attachment_digest = try manifests.attachmentDigest(state) }, .attachments = state, .routines = try a.dupe(manifests.Routine, &.{.{ .reference = reference, .record = .{ .id = 9, .generation = 1, .definition = .{ .returns_trigger = true, .arguments = &.{}, .language = .plpgsql, .body = "BEGIN RAISE NOTICE 'statement fired'; RETURN NEW; END", .owner = .auth_disabled } } }}) };
    return .{ .program = program, .processed = .{ .header = .{ .statement_id = @splat(4), .metadata_incarnation = program.metadata_incarnation, .table = program.table, .program_digest = try program.digest(), .events = .{ .update = true } }, .rows = try a.dupe(handoff.Row, &.{.{ .key = "row", .event = .update, .preimage = .{ .present = .{ .version = 7, .digest = @splat(3) } }, .after = try handoff.rowDigest("row", "{\"n\":42}", &.{}) }}) } };
}

/// Shared fixture for the actual configured API signer/native verifier test.
pub fn configuredAuthorityFixture(a: std.mem.Allocator) !struct { captured: handoff.Receipt, view: View } {
    const captured = try receipt(a);
    return .{ .captured = captured, .view = .{ .state = captured.program.attachments.? } };
}

/// Deliberately unsigned fixture for checking the actual opaque owner's group
/// fence before any later schema or user-authority validation can mask it.
pub fn ownerGroupFixture(a: std.mem.Allocator, group: u64) ![]u8 {
    var captured = try receipt(a);
    // A zero-row statement is valid in all three lanes. Ordinary replicated
    // batches intentionally cannot carry local, unscoped row predicates.
    captured.processed.rows = &.{};
    return batch.encodeBatchRequest(a, .{ .trigger_receipts = &.{captured}, .trigger_preparation_group_id = group, .trigger_preparation_admitted_at_seconds = 100, .trigger_preparation_proof = "group-fence-fixture" });
}

/// A correctly signed, deliberately stale immutable-schema command. The live
/// CAPI test distinguishes successful USER-key propagation from the subsequent
/// schema fence, without advertising or fabricating trigger activation.
pub fn ownerAuthorityFixture(a: std.mem.Allocator, group: u64, secret: []const u8, issuer: []const u8, now: i64) ![]u8 {
    const DynamicSigner = struct {
        secret: []const u8,
        issuer: []const u8,
        now: i64,
        fn sign(ptr: *anyopaque, alloc: std.mem.Allocator, scope_value: authority.Scope) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return authority.sign(alloc, self.secret, self.issuer, scope_value, self.now);
        }
    };
    var captured = try receipt(a);
    captured.processed.rows = &.{.{ .key = "row", .event = .insert, .preimage = .absent, .after = try handoff.rowDigest("row", "{\"n\":42}", &.{}) }};
    captured.processed.header.events = .{ .insert = true };
    var signer: DynamicSigner = .{ .secret = secret, .issuer = issuer, .now = now };
    const minter: authority.Minter = .{ .ptr = &signer, .sign_fn = DynamicSigner.sign, .table_id = 7, .table_fingerprint = try captured.program.table.fingerprint(), .receipts_digest = try handoff.receiptsDigest(a, &.{captured}), .command_fence_digest = @splat(8) };
    const request = try admission.seal(a, minter, group, now, .{ .schema_version = 1, .trigger_receipts = &.{captured}, .writes = &.{.{ .key = "row", .value = "{\"n\":42}" }}, .predicates = &.{.{ .key = "row", .expected_version = 0 }} });
    return batch.encodeOwnerBatchRequest(a, request);
}

pub fn checkPreparationAuthority(allocator: std.mem.Allocator) !void {
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const captured = try receipt(a);
    var marker: u8 = 0;
    const minter: authority.Minter = .{ .ptr = &marker, .sign_fn = Signer.sign, .table_id = 7, .table_fingerprint = try captured.program.table.fingerprint(), .receipts_digest = try handoff.receiptsDigest(a, &.{captured}), .command_fence_digest = @splat(8) };
    const req = try admission.seal(a, minter, 11, 100, .{ .trigger_receipts = &.{captured}, .schema_version = 3, .writes = &.{.{ .key = "row", .value = "{\"n\":42}" }}, .predicates = &.{.{ .key = "row", .expected_version = 7, .expected_content_digest = @splat(3) }} });
    const view: View = .{ .state = captured.program.attachments.? };
    try admission.requireOwnerGroup(req, 11);
    try admission.requireSchemaView(req, true);
    try admission.requireOwnerTable(req, 7);
    try std.testing.expectError(error.CatalogGenerationChanged, admission.requireSchemaView(req, false));
    try std.testing.expectError(error.CatalogGenerationChanged, admission.requireOwnerTable(req, 8));
    try admission.requireSchemaView(.{}, false);
    try admission.verify(a, view, req, "actual-user-authority-key", "issuer", 101, false);
    try std.testing.expectError(error.PreparedGenerationChanged, admission.requireOwnerGroup(req, 12));
    try std.testing.expectError(error.Forbidden, admission.verify(a, view, req, "node-transport-secret", "issuer", 101, false));
    try std.testing.expectError(error.Forbidden, admission.verify(a, view, req, "actual-user-authority-key", "issuer", 130, false));
    // The existing ordered-owner receipt proves durable admission. Recovery
    // validates immutable program/row evidence using that original instant;
    // it never invokes a VM, emits another NOTICE or re-samples a clock.
    try admission.verify(a, view, req, "actual-user-authority-key", "issuer", 130, true);
    try admission.verify(a, view, req, "rotated-current-key", "rotated-issuer", 130, true);
    try admission.verify(a, view, req, null, null, 130, true);
    const transform_only: types.BatchRequest = .{ .transforms = &.{.{ .key = "row", .operations = &.{.{ .op = .set, .path = "n", .value_json = "43" }} }} };
    try std.testing.expectError(error.SqlTriggerCoordinationRequired, admission.verify(a, view, transform_only, null, null, 101, false));
    var plain = view;
    plain.state.entries = &.{};
    try admission.verify(a, plain, transform_only, null, null, 101, false);
    var changed = req;
    changed.writes = &.{.{ .key = "row", .value = "{\"n\":43}" }};
    try std.testing.expectError(error.Forbidden, admission.verify(a, view, changed, "actual-user-authority-key", "issuer", 101, false));
    changed = req;
    changed.predicates = &.{.{ .key = "row", .expected_version = 8, .expected_content_digest = @splat(3) }};
    try std.testing.expectError(error.Forbidden, admission.verify(a, view, changed, "actual-user-authority-key", "issuer", 101, false));
    var stale = view;
    stale.schema_digest[0] ^= 1;
    try std.testing.expectError(error.CatalogGenerationChanged, admission.verify(a, stale, req, "actual-user-authority-key", "issuer", 101, false));
    try std.testing.expectError(error.SqlTriggerCoordinationRequired, admission.verify(a, view, .{ .writes = req.writes }, null, null, 101, false));
    try checkLogicalMutationChain(a);
    try checkStatementOnlyReceipt(a);
}

fn checkStatementOnlyReceipt(a: std.mem.Allocator) !void {
    var captured = try receipt(a);
    captured.processed.rows = &.{};
    const writes: []const types.TransactionWrite = &.{};
    const ordinary = try handoff.forParticipant(a, &.{captured}, writes, &.{});
    try std.testing.expectEqual(@as(usize, 0), ordinary.len);
    const anchor = try handoff.forParticipantWithStatements(a, &.{captured}, writes, &.{}, true);
    try std.testing.expectEqual(@as(usize, 1), anchor.len);
    try std.testing.expectEqual(@as(usize, 0), anchor[0].processed.rows.len);
    var marker: u8 = 0;
    const minter: authority.Minter = .{ .ptr = &marker, .sign_fn = Signer.sign, .table_id = 7, .table_fingerprint = try captured.program.table.fingerprint(), .receipts_digest = try handoff.receiptsDigest(a, anchor), .command_fence_digest = @splat(8) };
    const request = try admission.seal(a, minter, 11, 100, .{ .schema_version = 3, .trigger_receipts = anchor, .transaction = .{ .prepare = .{ .txn_id = @splat(5), .topology_epoch = 1 } } });
    const view: View = .{ .state = captured.program.attachments.? };
    try admission.verify(a, view, request, "actual-user-authority-key", "issuer", 101, false);
    try admission.verify(a, view, request, null, null, 500, true);
    var changed = view;
    changed.schema_digest[0] ^= 1;
    try std.testing.expectError(error.CatalogGenerationChanged, admission.verify(a, changed, request, "actual-user-authority-key", "issuer", 101, false));
    // Even empty effects carry one ordered statement completion, not a free
    // pass for duplicate replay evidence or a gap in a producer wave chain.
    try std.testing.expectError(error.InvalidSqlTriggerManifest, handoff.verifyPrepared(a, view, &.{ captured, captured }, true, writes, &.{}, @as([]const types.TransactionVersionPredicate, &.{})));
    captured.processed.header.sequence = 1;
    try std.testing.expectError(error.InvalidSqlTriggerManifest, handoff.verifyPrepared(a, view, &.{captured}, true, writes, &.{}, @as([]const types.TransactionVersionPredicate, &.{})));
}

fn checkLogicalMutationChain(a: std.mem.Allocator) !void {
    var inserted = try receipt(a);
    inserted.processed.header.events = .{ .insert = true, .update = true, .delete = true };
    inserted.processed.rows = &.{.{ .key = "row", .event = .insert, .preimage = .absent, .after = try handoff.rowDigest("row", "{\"n\":1}", &.{}) }};
    var updated = inserted;
    updated.processed.header.sequence = 1;
    updated.processed.rows = &.{.{ .key = "row", .event = .update, .preimage = .absent, .after = try handoff.rowDigest("row", "{\"n\":2}", &.{}) }};
    const view: View = .{ .state = inserted.program.attachments.? };
    const writes: []const types.TransactionWrite = &.{.{ .key = "row", .value = "{\"n\":2}" }};
    const predicates: []const types.TransactionVersionPredicate = &.{.{ .key = "row", .expected_version = 0 }};
    // One absence predicate protects both logical events. Recovery consumes
    // only their final bytes and never runs either trigger a second time.
    try handoff.verifyPrepared(a, view, &.{ inserted, updated }, true, writes, &.{}, predicates);
    try std.testing.expectError(error.InvalidSqlTriggerManifest, handoff.verifyPrepared(a, view, &.{updated}, true, writes, &.{}, predicates));
    try std.testing.expectError(error.InvalidSqlTriggerManifest, handoff.verifyPrepared(a, view, &.{ updated, inserted }, true, writes, &.{}, predicates));
    try std.testing.expectError(error.InvalidSqlTriggerManifest, handoff.verifyPrepared(a, view, &.{ inserted, inserted }, true, writes, &.{}, predicates));
    var forged = updated;
    forged.processed.rows = &.{.{ .key = "row", .event = .update, .preimage = .{ .present = .{ .version = 7, .digest = @splat(3) } }, .after = updated.processed.rows[0].after }};
    try std.testing.expectError(error.InvalidSqlTriggerManifest, handoff.verifyPrepared(a, view, &.{ inserted, forged }, true, writes, &.{}, predicates));
    var deleted = updated;
    deleted.processed.header.sequence = 2;
    deleted.processed.rows = &.{.{ .key = "row", .event = .delete, .preimage = .absent, .after = null }};
    try handoff.verifyPrepared(a, view, &.{ inserted, updated, deleted }, true, @as([]const types.TransactionWrite, &.{}), &.{"row"}, predicates);
    // Deleting and reinserting a physically present row is the dual case:
    // its INSERT keeps the original present-row predicate, not a false absence.
    var old_delete = deleted;
    old_delete.processed.header.sequence = 0;
    old_delete.processed.rows = &.{.{ .key = "row", .event = .delete, .preimage = .{ .present = .{ .version = 7, .digest = @splat(3) } }, .after = null }};
    var reinserted = inserted;
    reinserted.processed.header.sequence = 1;
    reinserted.processed.rows = &.{.{ .key = "row", .event = .insert, .preimage = old_delete.processed.rows[0].preimage, .after = updated.processed.rows[0].after }};
    try handoff.verifyPrepared(a, view, &.{ old_delete, reinserted }, true, writes, &.{}, @as([]const types.TransactionVersionPredicate, &.{.{ .key = "row", .expected_version = 7, .expected_content_digest = @splat(3) }}));
}

pub fn checkNativeWireOwnership(allocator: std.mem.Allocator) !void {
    var arena: std.heap.ArenaAllocator = .init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const req: types.BatchRequest = .{ .trigger_receipts = &.{try receipt(a)}, .trigger_preparation_group_id = 11, .trigger_preparation_admitted_at_seconds = 100, .trigger_preparation_command_digest = @splat(8), .trigger_preparation_proof = "owned-native-proof", .writes = &.{.{ .key = "row", .value = "{\"n\":42}" }} };
    const bytes = try batch.encodeBatchRequest(a, req);
    try std.testing.expect(std.mem.startsWith(u8, bytes, "[\"row-semantics-batch-v3\","));
    try std.testing.expectError(error.InvalidBatchRequest, batch.parseBatchRequest(a, bytes));
    var parsed = try batch.parseInternalBatchRequest(allocator, bytes);
    defer parsed.deinit(allocator);
    const expected = try handoff.receiptsDigest(a, req.trigger_receipts);
    try std.testing.expectEqual(expected, try handoff.receiptsDigest(a, parsed.req.trigger_receipts));
    try std.testing.expectEqualStrings("owned-native-proof", parsed.req.trigger_preparation_proof);
    @memset(bytes, 'x');
    try std.testing.expectEqual(expected, try handoff.receiptsDigest(a, parsed.req.trigger_receipts));
    try std.testing.expectEqualStrings("owned-native-proof", parsed.req.trigger_preparation_proof);
    const downgraded = try batch.encodeBatchRequest(a, req);
    const version = std.mem.indexOf(u8, downgraded, "batch-v3") orelse unreachable;
    downgraded[version + 7] = '2';
    try std.testing.expectError(error.InvalidBatchRequest, batch.parseInternalBatchRequest(a, downgraded));
}

test "trigger native preparation fences user authority owner body preimages and no-refire ordered replay" {
    try checkPreparationAuthority(std.testing.allocator);
}
test "trigger native v3 wire owns receipt program and proof and forbids public or downgraded admission" {
    try checkNativeWireOwnership(std.testing.allocator);
}

test "graph preparation native absence receipts require actual schema absence exact source proof and protected final body" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph: @import("../storage/db/prepared_graph_effects.zig").Admission = .{ .metadata_group_id = 11, .incarnation = @splat(1), .source_revision = 9, .table_id = 7, .schema_digest = @splat(2), .indexes = &.{.{ .name = "g", .digest = @splat(3) }} };
    const absent = try manifests.Absence.fromAdmission(graph, 3);
    const program: manifests.Manifest = .{ .format = 2, .metadata_incarnation = @splat(1), .catalog_revision = 9, .table = try absent.table(), .absence = absent, .routines = &.{} };
    const captured: handoff.Receipt = .{ .program = program, .processed = .{ .header = .{ .statement_id = @splat(4), .metadata_incarnation = program.metadata_incarnation, .table = program.table, .program_digest = try program.digest(), .events = .{ .update = true } }, .rows = &.{.{ .key = "row", .event = .update, .preimage = .{ .present = .{ .version = 7, .digest = @splat(3) } }, .after = try handoff.rowDigest("row", "{\"n\":42}", &.{}) }} } };
    var marker: u8 = 0;
    const minter: authority.Minter = .{ .ptr = &marker, .sign_fn = Signer.sign, .table_id = 7, .table_fingerprint = try program.table.fingerprint(), .receipts_digest = try handoff.receiptsDigest(a, &.{captured}) };
    const request = try admission.seal(a, minter, 11, 100, .{ .graph_preparation = graph, .trigger_receipts = &.{captured}, .schema_version = 3, .writes = &.{.{ .key = "row", .value = "{\"n\":42}" }}, .predicates = &.{.{ .key = "row", .expected_version = 7, .expected_content_digest = @splat(3) }}, .graph_writes = &.{.{ .index_name = "g", .source = "row", .target = "b", .edge_type = "R" }} });
    const AbsentView = struct {
        state: ?attachments.State = null,
        pub fn triggerState(self: *const @This()) ?*const attachments.State {
            return if (self.state) |*value| value else null;
        }
        pub fn version(_: @This()) u32 {
            return 3;
        }
        pub fn validator(_: @This()) ?struct { source_digest: ?[32]u8 } {
            return .{ .source_digest = @splat(2) };
        }
    };
    const view: AbsentView = .{};
    try admission.requireOwnerTable(request, 7);
    try admission.requireOwnerGroup(request, 11);
    try admission.verify(a, view, request, "actual-user-authority-key", "issuer", 101, false);
    try admission.verify(a, view, request, null, null, 999, true);
    const newly_attached: AbsentView = .{ .state = .{ .table_id = 7, .metadata_incarnation = @splat(1), .generation = 1 } };
    try std.testing.expectError(error.CatalogGenerationChanged, admission.verify(a, newly_attached, request, "actual-user-authority-key", "issuer", 101, false));
    var missing = request;
    missing.graph_preparation = null;
    try std.testing.expectError(error.SqlTriggerCoordinationRequired, admission.verify(a, view, missing, "actual-user-authority-key", "issuer", 101, false));
    var substituted = request;
    substituted.graph_preparation.?.source_revision += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, admission.verify(a, view, substituted, "actual-user-authority-key", "issuer", 101, false));
    substituted = request;
    substituted.graph_writes = &.{.{ .index_name = "g", .source = "row", .target = "different", .edge_type = "R" }};
    try std.testing.expectError(error.Forbidden, admission.verify(a, view, substituted, "actual-user-authority-key", "issuer", 101, false));
}

test "graph preparation schema NONE is distinct from version zero schema and exact protected no-refire effects" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph: @import("../storage/db/prepared_graph_effects.zig").Admission = .{ .metadata_group_id = 11, .incarnation = @splat(1), .source_revision = 9, .table_id = 7, .schema_digest = @import("../storage/db/relational_schema_owner_contract.zig").hash(""), .indexes = &.{.{ .name = "g", .digest = @splat(3) }} };
    var absent = try manifests.Absence.fromAdmission(graph, 0);
    absent.schema_present = false;
    try absent.requireNoSchema();
    const program: manifests.Manifest = .{ .format = 2, .metadata_incarnation = graph.incarnation, .catalog_revision = 9, .table = try absent.table(), .absence = absent, .routines = &.{} };
    const captured: handoff.Receipt = .{ .program = program, .processed = .{ .header = .{ .statement_id = @splat(4), .metadata_incarnation = program.metadata_incarnation, .table = program.table, .program_digest = try program.digest(), .events = .{ .insert = true } }, .rows = &.{.{ .key = "row", .event = .insert, .preimage = .absent, .after = try handoff.rowDigest("row", "{\"n\":42}", &.{}) }} } };
    var marker: u8 = 0;
    const minter: authority.Minter = .{ .ptr = &marker, .sign_fn = Signer.sign, .table_id = 7, .table_fingerprint = try program.table.fingerprint(), .receipts_digest = try handoff.receiptsDigest(a, &.{captured}) };
    const request = try admission.seal(a, minter, 11, 100, .{ .graph_preparation = graph, .trigger_receipts = &.{captured}, .writes = &.{.{ .key = "row", .value = "{\"n\":42}" }}, .predicates = &.{.{ .key = "row", .expected_version = 0 }}, .graph_writes = &.{.{ .index_name = "g", .source = "row", .target = "b", .edge_type = "R" }} });
    try admission.verifyNoSchema(a, request, "actual-user-authority-key", "issuer", 101, false);
    try admission.verifyNoSchema(a, request, null, null, 999, true);
    try std.testing.expectError(error.Forbidden, admission.verifyNoSchema(a, request, "transport-is-not-user-authority", "issuer", 101, false));
    var wrong = request;
    wrong.schema_version = 0;
    try std.testing.expectError(error.SqlTriggerCoordinationRequired, admission.verifyNoSchema(a, wrong, "actual-user-authority-key", "issuer", 101, false));
    wrong = request;
    wrong.graph_writes = &.{.{ .index_name = "g", .source = "row", .target = "changed", .edge_type = "R" }};
    try std.testing.expectError(error.Forbidden, admission.verifyNoSchema(a, wrong, "actual-user-authority-key", "issuer", 101, false));
    wrong = request;
    wrong.graph_preparation.?.source_revision += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, admission.verifyNoSchema(a, wrong, "actual-user-authority-key", "issuer", 101, false));
    // A genuinely installed version-zero schema is not schema NONE. Neither
    // the caller's version nor absent trigger State may erase that distinction.
    const ZeroView = struct {
        pub fn triggerState(_: @This()) ?*const attachments.State {
            return null;
        }
        pub fn version(_: @This()) u32 {
            return 0;
        }
        pub fn validator(_: @This()) ?struct { source_digest: ?[32]u8 } {
            return .{ .source_digest = @import("../storage/db/relational_schema_owner_contract.zig").hash("") };
        }
    };
    try std.testing.expectError(error.CatalogGenerationChanged, admission.verify(a, ZeroView{}, request, "actual-user-authority-key", "issuer", 101, false));
    const encoded = try handoff.encodeReceipts(a, &.{captured});
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"schema_present\":false") != null);
    var roundtrip = try handoff.decodeReceipts(a, encoded);
    defer roundtrip.deinit();
    try std.testing.expectEqualDeep(captured, roundtrip.value[0]);
    // Omitting the explicit NONE witness must not turn an old schema receipt
    // into permission to write to a schema-free replacement owner.
    var declared = captured;
    declared.program.absence.?.schema_present = true;
    try std.testing.expectError(error.CatalogGenerationChanged, admission.requireSchemaView(.{ .trigger_receipts = &.{declared} }, false));
}
