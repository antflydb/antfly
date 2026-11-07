// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const client = @import("kernel_owner_client.zig");
const fixtures = @import("antfly_local_sources").sql_catalog.trigger_native_admission_tests;

test "opaque trigger preparation binds actual owner group across live and replicated entrypoints" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    var context: client.Context = .{};
    try context.ensure();
    defer context.deinit();
    var owner = try client.Owner.open(.{
        .context = context.handle,
        .path = .fromSlice(path),
        .table_name = .fromSlice("rows"),
        .group_id = 72,
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 172,
        .identity_range_id = 272,
        .schema_json = .fromSlice(
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
        ),
    });
    defer owner.deinit();
    // A shard number is not a group identity even when it is otherwise valid.
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const wrong = try fixtures.ownerGroupFixture(arena.allocator(), 172);
    try std.testing.expectError(error.PreparedGenerationChanged, owner.batchJson("rows", wrong));
    try std.testing.expectError(error.PreparedGenerationChanged, owner.replicatedBatchJson("rows", wrong));
    try std.testing.expectError(error.PreparedGenerationChanged, owner.replicatedBatchAtRaftEntryJson("rows", wrong, 1, 1));
    // Matching the group reaches independent immutable schema validation. It
    // does not permit an unsigned foreign attachment to run on this owner.
    const matching = try fixtures.ownerGroupFixture(arena.allocator(), 72);
    try std.testing.expectError(error.CatalogGenerationChanged, owner.replicatedBatchAtRaftEntryJson("rows", matching, 1, 1));
    var status = try owner.runtimeStatusJson("rows");
    defer status.deinit();
    try std.testing.expect(std.mem.indexOf(u8, status.bytes(), "\"source_doc_count\":0") != null);
}

// Component qualification, not a claim that schema 1 has activated triggers:
// correct USER credentials pass authentication and meet the immutable-schema
// rejection; distinct/wrong credentials stop earlier. Every lane stays empty.
test "opaque SQL preparation USER keys survive the actual CAPI owner boundary and remain separate from row policy" {
    const a = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const now: i64 = @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s));
    const wire = try fixtures.ownerAuthorityFixture(alloc, 72, "dedicated-sql-user-secret", "dedicated-sql-user-issuer", now);
    var context: client.Context = .{};
    try context.ensure();
    defer context.deinit();
    // The no-dedicated control has a different configured policy pair. Legacy
    // fallback is valid, but cannot authenticate this dedicated SQL grant.
    for ([_]enum { correct, wrong, absent, no_policy }{ .correct, .wrong, .absent, .no_policy }) |mode| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
        defer a.free(path);
        const selected = if (mode == .wrong) "different-sql-user-secret" else "dedicated-sql-user-secret";
        const owned_secret = try a.dupe(u8, selected);
        defer a.free(owned_secret);
        const owned_issuer = try a.dupe(u8, "dedicated-sql-user-issuer");
        defer a.free(owned_issuer);
        var owner = try client.Owner.open(.{
            .context = context.handle,
            .path = .fromSlice(path),
            .table_name = .fromSlice("rows"),
            .group_id = 72,
            .has_identity_namespace = 1,
            .identity_table_id = 7,
            .identity_shard_id = 172,
            .identity_range_id = 272,
            .sql_preparation_secret = .fromSlice(if (mode == .absent) "" else owned_secret),
            .sql_preparation_issuer = .fromSlice(if (mode == .absent) "" else owned_issuer),
            .row_policy_authority_secret = .fromSlice(if (mode == .no_policy) "" else "distinct-public-row-policy-secret"),
            .row_policy_authority_issuer = .fromSlice(if (mode == .no_policy) "" else "public-row-policy-issuer"),
            .schema_json = .fromSlice(
                \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
            ),
        });
        defer owner.deinit();
        // OpenRequest is borrowed only during open: the actual native owner
        // must retain private key bytes independently until close drains.
        @memset(owned_secret, 'x');
        @memset(owned_issuer, 'x');
        if (mode == .correct or mode == .no_policy) {
            try std.testing.expectError(error.CatalogGenerationChanged, owner.batchJson("rows", wire));
        } else {
            try std.testing.expectError(error.Forbidden, owner.batchJson("rows", wire));
        }
        if (mode == .no_policy) {
            // A SQL USER key is not an RLS verifier. This real lookup reaches
            // the normal row-policy verifier and observes its absent key.
            try std.testing.expectError(error.RowPolicyAuthorityUnavailable, owner.lookupJson("rows", "{\"key\":\"row\",\"row_policy_principal_proof\":\"not-a-policy-proof\",\"row_policy_database\":\"antfly\"}"));
        } else {
            // With the distinct RLS key configured, a malformed proof reaches
            // authentication and fails there rather than claiming no verifier.
            try std.testing.expectError(error.Forbidden, owner.lookupJson("rows", "{\"key\":\"row\",\"row_policy_principal_proof\":\"not-a-policy-proof\",\"row_policy_database\":\"antfly\"}"));
        }
        try std.testing.expectError(error.NotFound, owner.lookupJson("rows", "{\"key\":\"row\"}"));
        var status = try owner.runtimeStatusJson("rows");
        defer status.deinit();
        try std.testing.expect(std.mem.indexOf(u8, status.bytes(), "\"source_doc_count\":0") != null);
    }
}

test "opaque graph decoder ABI rejects ABI115 context and batch ingress before effects" {
    const a = std.testing.allocator;
    const abi = @import("kernel_owner_abi");
    try std.testing.expect(abi.abi_version >= abi.prepared_graph_effects_abi_version);
    var rejected_context: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_context_create(&.{ .version = 115 }, &rejected_context));
    try std.testing.expect(rejected_context == null);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/graph-decoder-abi", .{tmp.sub_path});
    defer a.free(path);
    var context: client.Context = .{};
    try context.ensure();
    defer context.deinit();
    var owner = try client.Owner.open(.{ .context = context.handle, .path = .fromSlice(path), .table_name = .fromSlice("rows"), .group_id = 72 });
    defer owner.deinit();
    // This is the existing v3 present-State wrapper, not the absence-v2
    // variant which an old decoder might reject for an incidental reason.
    // The receipt fixture owns nested allocations in its caller region.
    var fixture_arena = std.heap.ArenaAllocator.init(a);
    defer fixture_arena.deinit();
    const base = try fixtures.ownerGroupFixture(fixture_arena.allocator(), 72);
    const proof: @import("antfly_local_sources").storage_db_prepared_graph_effects.Admission = .{ .metadata_group_id = 1, .incarnation = @splat(1), .source_revision = 1, .table_id = 7, .schema_digest = @splat(2), .indexes = &.{.{ .name = "g", .digest = @splat(3) }} };
    const body = try std.fmt.allocPrint(a, "{{\"_graph_preparation\":{f},{s}", .{ std.json.fmt(proof, .{}), base[1..] });
    defer a.free(body);
    var response: abi.OwnedBytes = .{};
    defer abi.antfly_storage_owner_buffer_destroy(&response);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_batch_json(owner.handle, &.{ .version = 115, .table_name = .fromSlice("rows"), .request_json = .fromSlice(body) }, &response));
    try std.testing.expect(response.ptr == null and response.len == 0);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_replicated_batch_json(owner.handle, &.{ .version = 115, .table_name = .fromSlice("rows"), .request_json = .fromSlice(body) }, &response));
    try std.testing.expect(response.ptr == null and response.len == 0);
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_replicated_batch_at_raft_entry_json(owner.handle, &.{ .version = 115, .table_name = .fromSlice("rows"), .request_json = .fromSlice(body), .raft_term = 1, .raft_index = 1 }, &response));
    try std.testing.expect(response.ptr == null and response.len == 0);
    var empty = try owner.runtimeStatusJson("rows");
    defer empty.deinit();
    try std.testing.expect(std.mem.indexOf(u8, empty.bytes(), "\"source_doc_count\":0") != null);
    // Current requests still reach the real owner; a mismatch rejection is
    // neither a blanket write prohibition nor evidence of a completed effect.
    var accepted = try owner.batchJson("rows", "{\"inserts\":{\"probe\":{\"n\":1}}}");
    accepted.deinit();
    var found = try owner.lookupJson("rows", "{\"key\":\"probe\"}");
    defer found.deinit();
    try std.testing.expect(std.mem.indexOf(u8, found.bytes(), "\"n\":1") != null);
}

test "opaque privacy decoder ABI rejects ABI117 ABI118 and ABI119 context and lookup before effects" {
    const a = std.testing.allocator;
    const abi = @import("kernel_owner_abi");
    try std.testing.expectEqual(@as(u32, 120), abi.abi_version);
    var rejected_context: ?*anyopaque = null;
    for ([_]u32{ 117, 118, 119 }) |old_version| {
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_context_create(&.{ .version = old_version }, &rejected_context));
    }
    try std.testing.expect(rejected_context == null);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/privacy-decoder-abi", .{tmp.sub_path});
    defer a.free(path);
    var context: client.Context = .{};
    try context.ensure();
    defer context.deinit();
    var owner = try client.Owner.open(.{ .context = context.handle, .path = .fromSlice(path), .table_name = .fromSlice("rows"), .group_id = 72 });
    defer owner.deinit();
    var response: abi.OwnedBytes = .{};
    defer abi.antfly_storage_owner_buffer_destroy(&response);
    // The combined profile must reject before looking at privacy flags or
    // dereferencing the old provider's opaque JSON payload.
    for ([_]u32{ 117, 118, 119 }) |old_version| {
        try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_lookup_json(owner.handle, &.{ .version = old_version, .table_name = .fromSlice("rows"), .request_json = .{ .ptr = @ptrFromInt(1), .len = 64 } }, &response));
    }
    try std.testing.expect(response.ptr == null and response.len == 0);
    var accepted = try owner.batchJson("rows", "{\"inserts\":{\"probe\":{\"n\":1}}}");
    accepted.deinit();
    var found = try owner.lookupJson("rows", "{\"key\":\"probe\"}");
    defer found.deinit();
    try std.testing.expect(std.mem.indexOf(u8, found.bytes(), "\"n\":1") != null);
}
