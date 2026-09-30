// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const client = @import("kernel_owner_client.zig");
const db_mod = @import("db/db.zig");
const staging = @import("db/restore_staging_contract.zig");
const handoff = @import("db/empty_generation_handoff.zig");

test "storage owner handoff receipt survives shared-context hidden to public reopen" {
    const alloc = std.testing.allocator;
    const public_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const namespace: @import("db/doc_identity_namespace.zig").Namespace = .{ .table_id = 72, .shard_id = 7201, .range_id = 7201 };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owner-handoff-reopen", .{tmp.sub_path});
    defer alloc.free(path);
    var parsed_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, public_schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try @import("../schema/mod.zig").deriveRuntimeTableSchema(alloc, parsed_schema);
    defer @import("schema.zig").freeSchema(alloc, runtime_schema);
    const schema_bytes = try @import("schema.zig").serializeSchema(alloc, runtime_schema);
    defer alloc.free(schema_bytes);
    const scope: staging.Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(0),
        .source_namespace = .{ .table_id = 71, .shard_id = 7101, .range_id = 7101 },
        .target_namespace = namespace,
        .target_schema_digest = staging.digest(schema_bytes),
        .empty_generation = true,
    };
    const install: handoff.Install = .{
        .scope = scope.digest(),
        .source_summary_digest = @splat(3),
        .retired_digest = @splat(4),
        .retired_count = 0,
        .mappings = &.{},
    };
    const bootstrap: staging.OwnerBootstrap = .{
        .scope = scope,
        .table_name = "docs",
        .schema_json = public_schema_json,
        .indexes_json = "{}",
        .byte_range = .{ .start = "", .end = "" },
        .empty_generation_handoff = .{
            .source_summary_digest = install.source_summary_digest,
            .retired_digest = install.retired_digest,
            .retired_count = install.retired_count,
            .expected_install_receipt_digest = try handoff.installReceiptDigest(install),
        },
    };
    {
        var seeded = try db_mod.DB.open(alloc, path, .{ .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer seeded.close();
        try @import("db/doc_identity.zig").writeNamespaceToStore(seeded.core.store, namespace);
        try seeded.setSchemaJson(alloc, public_schema_json);
        try seeded.reserveRestoreStagingScoped(alloc, scope);
        try seeded.installRestoreStagingBootstrap(alloc, bootstrap);
        const imported = try (staging.Progress{ .scope = scope, .phase = .imported, .rows_complete = true, .artifacts_complete = true }).encode(alloc);
        defer alloc.free(imported);
        try seeded.core.store.put(staging.key, imported);
        _ = try seeded.finishRestoreStaging(alloc, scope.digest(), .validated);
        var txn = try seeded.core.store.beginWriteTxn();
        var txn_open = true;
        defer if (txn_open) txn.abort();
        _ = try handoff.stageInstall(alloc, &txn, install, scope.plan_id, 7, 11);
        try txn.commit();
        txn_open = false;
        try seeded.core.store.sync(true);
    }
    const bootstrap_json = try std.json.Stringify.valueAlloc(alloc, bootstrap, .{});
    defer alloc.free(bootstrap_json);
    var context = client.Context{};
    try context.ensure();
    defer context.deinit();
    var options: abi.OpenRequest = .{
        .context = context.handle,
        .path = .fromSlice(path),
        .table_name = .fromSlice("docs"),
        .group_id = namespace.shard_id,
        .has_identity_namespace = 1,
        .identity_table_id = namespace.table_id,
        .identity_shard_id = namespace.shard_id,
        .identity_range_id = namespace.range_id,
        .schema_json = .fromSlice(public_schema_json),
        .indexes_json = .fromSlice("{}"),
        .restore_bootstrap_json = .fromSlice(bootstrap_json),
    };
    const lookup_contract = @import("../api/local_query_contract.zig");
    const hidden_request = try lookup_contract.encodeStorageKernelLookupRequest(alloc, "", .{
        .restore_staging_scope = scope.digest(),
        .restore_staging_plan_id = scope.plan_id,
        .relational_topology_json = "{\"mode\":\"generation_handoff_install\"}",
    });
    defer alloc.free(hidden_request);
    const published_request = try lookup_contract.encodeStorageKernelLookupRequest(alloc, "", .{
        .relational_topology_json = "{\"mode\":\"generation_handoff_install\"}",
    });
    defer alloc.free(published_request);
    {
        var hidden = try client.Owner.open(options);
        defer hidden.deinit();
        var result = try hidden.lookupJson("docs", hidden_request);
        defer result.deinit();
        var parsed = try std.json.parseFromSlice(?staging.GenerationAdmissionReceipt, alloc, result.bytes(), .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(u64, 11), (parsed.value orelse return error.GenerationHandoffInstallMissing).applied_index);
    }
    {
        var promoted = try db_mod.DB.open(alloc, path, .{ .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer promoted.close();
        _ = try promoted.finishRestoreStaging(alloc, scope.digest(), .published);
    }
    options.restore_bootstrap_json = .{};
    // The published catalog descriptor supplies its canonical schema. The
    // CAPI then enables stale-schema rejection and optional runtimes, unlike
    // the private bootstrap open above.
    options.schema_json = .fromSlice(public_schema_json);
    var published = try client.Owner.open(options);
    defer published.deinit();
    var result = try published.lookupJson("docs", published_request);
    defer result.deinit();
    var parsed = try std.json.parseFromSlice(?staging.GenerationAdmissionReceipt, alloc, result.bytes(), .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 11), (parsed.value orelse return error.GenerationHandoffInstallMissing).applied_index);
}
