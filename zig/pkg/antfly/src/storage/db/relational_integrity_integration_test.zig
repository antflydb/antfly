// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.
const std = @import("std");
const db_mod = @import("db.zig");
const integrity = @import("relational_integrity.zig");
const catalog = @import("relational_integrity_catalog.zig");
const tuples = @import("relational_index_keys.zig");

test "self-FK dual owner preserves one admission fence across begin stage and activation restarts" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/self-fk-dual-midphase", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 71, .shard_id = 41, .range_id = 41 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false };
    const old_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const next_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"self_fk","child_columns":["parent_id"],"parent_table":"nodes","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const topology = @import("relational_integrity_topology.zig");
    const admission = @import("relational_integrity_generation_admission.zig");
    var fence: topology.Fence = undefined;
    var transition: admission.Transition = undefined;
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, old_json);
        const old_catalog = try db.core.store.get(alloc, catalog.key);
        defer alloc.free(old_catalog);
        var old_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(old_catalog, &old_digest, .{});
        var parsed = try @import("../../schema/mod.zig").parseValidatedTableSchema(alloc, next_json);
        defer parsed.deinit(alloc);
        const runtime_schema = try @import("../../schema/mod.zig").deriveRuntimeTableSchema(alloc, parsed);
        defer @import("../schema.zig").freeSchema(alloc, runtime_schema);
        var prepared = try db.core.prepareSchemaMetadataPublishedChild(runtime_schema, &.{.{ .key = "\x00\x00__metadata__:schema_json", .value = next_json }});
        defer prepared.deinit();
        const generation = (prepared.integrity_catalog.?.catalog.find(.foreign_key, "self_fk") orelse return error.IntegrityCatalogChanged).generation;
        transition = .{ .child_table_id = namespace.table_id, .child_table_name = "nodes", .constraint_name = "self_fk", .expected_generation = null, .next_generation = generation, .plan_id = @splat(6), .decision_digest = @splat(7) };
        fence = .{ .role = .child_generation_dual, .transition_id = 11, .attempt = 1, .admission_epoch = 1, .peer_group_id = 41, .owner_group_id = 41, .namespace = namespace, .catalog_digest = old_digest };
        // A transaction admitted before the fence must resolve before the
        // dual-role parent can stage the new accepted generation.
        const old_txn = try db.beginTransaction(1_700_000_000_000_000_000);
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .begin, .fence = fence } }, .{ .term = 1, .index = 1 });
        try std.testing.expectError(error.TransactionTopologyBusy, db.applyRelationalTopologyControl(.{ .action = .stage_child_generation, .fence = fence, .child_generations = &.{transition} }, null));
        try db.abortTransaction(old_txn, 1_700_000_000_000_000_001);
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expect((try db.relationalTopologyStatus()).fence.?.eql(fence));
        try std.testing.expect(try db.childGenerationSourcePinsSchemaJson(next_json));
        try std.testing.expectError(error.ForeignKeyGenerationPublicationRequired, db.setSchemaJson(alloc, next_json));
        var split = fence;
        split.role = .split_source;
        split.transition_id = 99;
        split.admission_epoch += 1;
        try std.testing.expectError(error.IntegrityTopologyBusy, db.applyRelationalTopologyControl(.{ .action = .begin, .fence = split }, null));
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .begin, .fence = fence } }, .{ .term = 1, .index = 1 });
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .stage_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 2 });
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .stage_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 2 });
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expect((try db.relationalTopologyStatus()).fence.?.eql(fence));
        try std.testing.expect(try db.childGenerationSourcePinsSchemaJson(next_json));
        try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .action = .release, .fence = fence }, null));
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .activate_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 3 });
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .activate_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 3 });
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expect((try db.relationalTopologyStatus()).fence.?.eql(fence));
        try std.testing.expect(try db.childGenerationSourcePinsSchemaJson(next_json));
        try std.testing.expectError(error.GenerationAdmissionChanged, db.applyRelationalTopologyControl(.{ .action = .cancel, .fence = fence, .child_generations = &.{transition} }, null));
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.GenerationAdmissionAcknowledgementPending, admission.requireDualInstallReady(&read, fence));
        }
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .acknowledge_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 4 });
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .acknowledge_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 4 });
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expect((try db.relationalTopologyStatus()).fence.?.eql(fence));
        try std.testing.expect(try db.childGenerationSourcePinsSchemaJson(next_json));
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try admission.requireDualInstallReady(&read, fence);
    }
}

test "self-FK owner retains one fence through parent ACK and atomic child install across restart" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/self-fk-dual-owner", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 71, .shard_id = 41, .range_id = 41 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false };
    const old_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const next_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"self_fk","child_columns":["parent_id"],"parent_table":"nodes","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const topology = @import("relational_integrity_topology.zig");
    const admission = @import("relational_integrity_generation_admission.zig");
    var transition: admission.Transition = undefined;
    var fence: topology.Fence = undefined;
    var before_digest: [32]u8 = undefined;
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, old_json);
        const old_catalog = try db.core.store.get(alloc, catalog.key);
        defer alloc.free(old_catalog);
        std.crypto.hash.Blake3.hash(old_catalog, &before_digest, .{});
        var next_public = try @import("../../schema/mod.zig").parseValidatedTableSchema(alloc, next_json);
        defer next_public.deinit(alloc);
        const next_runtime = try @import("../../schema/mod.zig").deriveRuntimeTableSchema(alloc, next_public);
        defer @import("../schema.zig").freeSchema(alloc, next_runtime);
        var next_prepared = try db.core.prepareSchemaMetadataPublishedChild(next_runtime, &.{.{ .key = "\x00\x00__metadata__:schema_json", .value = next_json }});
        defer next_prepared.deinit();
        const next_generation = (next_prepared.integrity_catalog.?.catalog.find(.foreign_key, "self_fk") orelse return error.IntegrityCatalogChanged).generation;
        transition = .{ .child_table_id = 71, .child_table_name = "nodes", .constraint_name = "self_fk", .expected_generation = null, .next_generation = next_generation, .plan_id = @splat(6), .decision_digest = @splat(7) };
        fence = .{ .role = .child_generation_dual, .transition_id = 11, .attempt = 1, .admission_epoch = 2, .peer_group_id = 41, .owner_group_id = 41, .namespace = namespace, .catalog_digest = before_digest };
        var abandoned = fence;
        abandoned.transition_id = 10;
        abandoned.admission_epoch = 1;
        try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = abandoned }, null);
        try db.applyRelationalTopologyControl(.{ .action = .stage_child_generation, .fence = abandoned, .child_generations = &.{transition} }, null);
        try std.testing.expectError(error.GenerationAdmissionPending, db.applyRelationalTopologyControl(.{ .action = .cancel_child_generation_source, .fence = abandoned }, null));
        try db.applyRelationalTopologyControl(.{ .action = .cancel, .fence = abandoned, .child_generations = &.{transition} }, null);
        try db.applyRelationalTopologyControl(.{ .action = .cancel_child_generation_source, .fence = abandoned }, null);
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .begin, .fence = fence } }, .{ .term = 1, .index = 1 });
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .stage_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 2 });
        try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .action = .release, .fence = fence }, null));
        try std.testing.expectError(error.ForeignKeyGenerationPublicationRequired, db.setSchemaJson(alloc, next_json));
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .activate_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 3 });
        try std.testing.expect((try db.relationalTopologyStatus()).fence.?.eql(fence));
        try std.testing.expectError(error.GenerationAdmissionChanged, db.applyRelationalTopologyControl(.{ .action = .cancel, .fence = fence, .child_generations = &.{transition} }, null));
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.GenerationAdmissionAcknowledgementPending, admission.requireDualInstallReady(&read, fence));
        }
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .acknowledge_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 1, .index = 4 });
        try std.testing.expectError(error.InvalidIntegrityOperation, db.batch(.{ .deletes = &.{admission.dual_acknowledged_fence_key} }));
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try admission.requireDualInstallReady(&read, fence);
        try admission.requireDualCatalogMatch(alloc, &read, namespace.table_id, next_prepared.integrity_catalog.?.catalog);
        const forged_bindings = try alloc.dupe(catalog.Binding, next_prepared.integrity_catalog.?.catalog.bindings);
        defer alloc.free(forged_bindings);
        for (forged_bindings) |*candidate_binding| if (!candidate_binding.retired and candidate_binding.definition.kind == .foreign_key) {
            candidate_binding.generation = @splat(9);
            break;
        };
        var forged_catalog = next_prepared.integrity_catalog.?.catalog;
        forged_catalog.bindings = forged_bindings;
        try std.testing.expectError(error.GenerationAdmissionChanged, admission.requireDualCatalogMatch(alloc, &read, namespace.table_id, forged_catalog));
    }
    {
        var reopened = try db_mod.DB.open(alloc, path, options);
        defer reopened.close();
        try std.testing.expect(try reopened.childGenerationSourcePinsSchemaJson(next_json));
        {
            var read = try reopened.core.store.beginReadTxn();
            defer read.abort();
            try admission.requireDualInstallReady(&read, fence);
        }
        var parsed = try @import("../../schema/mod.zig").parseValidatedTableSchema(alloc, next_json);
        defer parsed.deinit(alloc);
        const runtime_schema = try @import("../../schema/mod.zig").deriveRuntimeTableSchema(alloc, parsed);
        defer @import("../schema.zig").freeSchema(alloc, runtime_schema);
        var prepared = try reopened.core.prepareSchemaMetadataPublishedChild(runtime_schema, &.{.{ .key = "\x00\x00__metadata__:schema_json", .value = next_json }});
        defer prepared.deinit();
        var after_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(prepared.integrity_catalog.?.value, &after_digest, .{});
        var old_json_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(old_json, &old_json_digest, .{});
        var next_json_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(next_json, &next_json_digest, .{});
        const command: topology.Command = .{ .action = .install_child_schema, .fence = fence, .child_schema_install = .{ .schema_json = next_json, .before_schema_json_digest = old_json_digest, .schema_json_digest = next_json_digest, .before_catalog_digest = before_digest, .after_catalog_digest = after_digest } };
        try reopened.batchRaftReplicatedApply(.{ .relational_topology = command }, .{ .term = 1, .index = 5 });
        try reopened.batchRaftReplicatedApply(.{ .relational_topology = command }, .{ .term = 1, .index = 5 });
        try std.testing.expect((try reopened.relationalTopologyStatus()).fence == null);
        {
            var read = try reopened.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.NotFound, read.get(admission.dual_acknowledged_fence_key));
        }
        const published = (try reopened.getSchemaJson(alloc)).?;
        defer alloc.free(published);
        try std.testing.expectEqualStrings(next_json, published);

        // Standby replay must not treat a parent activation as publication
        // authority. The exact dual-role ACK is durable across promotion and
        // the HA schema-cut record consumes it atomically with fence release.
        const standby_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/self-fk-dual-standby", .{tmp.sub_path});
        defer alloc.free(standby_path);
        const effects = @import("../hot_standby/effects.zig");
        const ha_payload = try effects.encodePublishedChildSchemaMetadataMutationAlloc(alloc, runtime_schema, next_json, .{
            .fence = fence,
            .before_schema_json_digest = old_json_digest,
            .schema_json_digest = next_json_digest,
            .before_catalog_digest = before_digest,
            .after_catalog_digest = after_digest,
            .applied_term = 1,
            .applied_index = 5,
        });
        defer alloc.free(ha_payload);
        const ha_record: @import("../hot_standby/replication_record.zig").Record = .{
            .kind = .metadata_mutation,
            .payload_codec = .json,
            .cluster_id = 1,
            .timeline_id = 1,
            .epoch = 1,
            .lsn = 1,
            .previous_lsn = 0,
            .payload = ha_payload,
        };
        {
            var standby = try db_mod.DB.open(alloc, standby_path, options);
            defer standby.close();
            try standby.setSchemaJson(alloc, old_json);
            try standby.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .begin, .fence = fence } }, .{ .term = 2, .index = 1 });
            try standby.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .stage_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 2, .index = 2 });
            try standby.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .activate_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 2, .index = 3 });
            try std.testing.expectError(error.GenerationAdmissionAcknowledgementPending, standby.applyHAReplicationRecord(ha_record));
            try std.testing.expect((try standby.relationalTopologyStatus()).fence.?.eql(fence));
            const old = (try standby.getSchemaJson(alloc)).?;
            defer alloc.free(old);
            try std.testing.expectEqualStrings(old_json, old);
        }
        {
            var standby = try db_mod.DB.open(alloc, standby_path, options);
            defer standby.close();
            try std.testing.expect((try standby.relationalTopologyStatus()).fence.?.eql(fence));
            try standby.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .acknowledge_child_generation, .fence = fence, .child_generations = &.{transition} } }, .{ .term = 2, .index = 4 });
            try standby.applyHAReplicationRecord(ha_record);
            try standby.applyHAReplicationRecord(ha_record);
            try std.testing.expect((try standby.relationalTopologyStatus()).fence == null);
            const after = (try standby.getSchemaJson(alloc)).?;
            defer alloc.free(after);
            try std.testing.expectEqualStrings(next_json, after);
        }
        {
            var promoted = try db_mod.DB.open(alloc, standby_path, options);
            defer promoted.close();
            try std.testing.expect((try promoted.relationalTopologyStatus()).fence == null);
            const after = (try promoted.getSchemaJson(alloc)).?;
            defer alloc.free(after);
            try std.testing.expectEqualStrings(next_json, after);
            const receipt = try promoted.core.store.get(alloc, admission.source_install_receipt_key);
            defer alloc.free(receipt);
            _ = try admission.AppliedReceipt.decode(receipt);
        }
    }
    var installed = try db_mod.DB.open(alloc, path, options);
    defer installed.close();
    const published = (try installed.getSchemaJson(alloc)).?;
    defer alloc.free(published);
    try std.testing.expectEqualStrings(next_json, published);
    try std.testing.expect((try installed.relationalTopologyStatus()).fence == null);

    // A corrupted internal activation cannot turn the valid metadata schema
    // digest into permission to expose an unmatched accepted generation.
    const bad_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/self-fk-dual-forged", .{tmp.sub_path});
    defer alloc.free(bad_path);
    var bad_fence = fence;
    bad_fence.transition_id = 31;
    bad_fence.admission_epoch = 1;
    var forged_transition = transition;
    forged_transition.next_generation = @splat(9);
    {
        var bad = try db_mod.DB.open(alloc, bad_path, options);
        defer bad.close();
        try bad.setSchemaJson(alloc, old_json);
        try bad.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .begin, .fence = bad_fence } }, .{ .term = 2, .index = 1 });
        try bad.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .stage_child_generation, .fence = bad_fence, .child_generations = &.{forged_transition} } }, .{ .term = 2, .index = 2 });
        try bad.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .activate_child_generation, .fence = bad_fence, .child_generations = &.{forged_transition} } }, .{ .term = 2, .index = 3 });
        try bad.batchRaftReplicatedApply(.{ .relational_topology = .{ .action = .acknowledge_child_generation, .fence = bad_fence, .child_generations = &.{forged_transition} } }, .{ .term = 2, .index = 4 });
    }
    {
        var bad = try db_mod.DB.open(alloc, bad_path, options);
        defer bad.close();
        var parsed = try @import("../../schema/mod.zig").parseValidatedTableSchema(alloc, next_json);
        defer parsed.deinit(alloc);
        const runtime_schema = try @import("../../schema/mod.zig").deriveRuntimeTableSchema(alloc, parsed);
        defer @import("../schema.zig").freeSchema(alloc, runtime_schema);
        var prepared = try bad.core.prepareSchemaMetadataPublishedChild(runtime_schema, &.{.{ .key = "\x00\x00__metadata__:schema_json", .value = next_json }});
        defer prepared.deinit();
        var after_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(prepared.integrity_catalog.?.value, &after_digest, .{});
        var old_json_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(old_json, &old_json_digest, .{});
        var next_json_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(next_json, &next_json_digest, .{});
        const install: topology.Command = .{ .action = .install_child_schema, .fence = bad_fence, .child_schema_install = .{ .schema_json = next_json, .before_schema_json_digest = old_json_digest, .schema_json_digest = next_json_digest, .before_catalog_digest = before_digest, .after_catalog_digest = after_digest } };
        try std.testing.expectError(error.GenerationAdmissionChanged, bad.batchRaftReplicatedApply(.{ .relational_topology = install }, .{ .term = 2, .index = 5 }));
        try std.testing.expect((try bad.relationalTopologyStatus()).fence.?.eql(bad_fence));
        const still_old = (try bad.getSchemaJson(alloc)).?;
        defer alloc.free(still_old);
        try std.testing.expectEqualStrings(old_json, still_old);
    }
}

test "initial partial FK parent support remains unready until every owner index survives restart" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/initial-partial-parent", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 201, .shard_id = 401, .range_id = 401 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false };
    const original =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["a","b"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer","nullable":true},"b":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    const support = @import("../../schema/relational_witness_indexes.zig");
    const patched = (try support.ensureCoverage(alloc, original, &.{ "a", "b" })).?;
    defer alloc.free(patched);
    const next = try std.mem.replaceOwned(u8, alloc, patched, "\"version\":1", "\"version\":2");
    defer alloc.free(next);
    var parsed = try @import("../../schema/mod.zig").parseValidatedTableSchema(alloc, next);
    defer parsed.deinit(alloc);
    const readiness = @import("../../api/relational_index_status.zig");
    const status_contract = @import("relational_index_status_contract.zig");
    const a_name = support.supportName("a");
    const b_name = support.supportName("b");
    const names = [_][]const u8{ &a_name, &b_name };
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, original);
        try db.setSchemaJson(alloc, next);
        for (names) |name| {
            const request = try std.json.Stringify.valueAlloc(alloc, status_contract.Request{ .name = name, .schema_version = 2 }, .{});
            defer alloc.free(request);
            var response = (try db.lookup(alloc, "", .{ .relational_index_status_json = request })).?;
            defer response.deinit(alloc);
            var status = try std.json.parseFromSlice(status_contract.Status, alloc, response.json, .{});
            defer status.deinit();
            const comparison = try readiness.expectedComparison(alloc, parsed, name);
            try std.testing.expectError(error.GenerationAdmissionPending, readiness.requireInitialFkSupportReady(status.value, 201, 2, "", "", comparison));
        }
        for (names) |name| {
            for (0..2048) |_| {
                if ((try db.relationalIndexBuildStatus(name)).state == .ready) break;
                try db.buildRelationalIndexStep(name, .{});
            }
            try std.testing.expectEqual(@import("relational_index_jobs.zig").State.ready, (try db.relationalIndexBuildStatus(name)).state);
        }
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        for (names) |name| {
            const request = try std.json.Stringify.valueAlloc(alloc, status_contract.Request{ .name = name, .schema_version = 2 }, .{});
            defer alloc.free(request);
            var response = (try db.lookup(alloc, "", .{ .relational_index_status_json = request })).?;
            defer response.deinit(alloc);
            var status = try std.json.parseFromSlice(status_contract.Status, alloc, response.json, .{});
            defer status.deinit();
            const comparison = try readiness.expectedComparison(alloc, parsed, name);
            try readiness.requireInitialFkSupportReady(status.value, 201, 2, "", "", comparison);
        }
    }
}

test "child FK generation schema install commits catalog and source release with Raft marker" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-generation-child", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 51, .shard_id = 41, .range_id = 41 };
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const before_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const after_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, before_json);
    const old_catalog = try db.core.store.get(alloc, catalog.key);
    defer alloc.free(old_catalog);
    var before_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(old_catalog, &before_digest, .{});
    const public_schema = @import("../../schema/mod.zig");
    var parsed = try public_schema.parseValidatedTableSchema(alloc, after_json);
    defer parsed.deinit(alloc);
    const runtime_schema = try public_schema.deriveRuntimeTableSchema(alloc, parsed);
    defer @import("../schema.zig").freeSchema(alloc, runtime_schema);
    var prepared = try db.core.prepareSchemaMetadataPublishedChild(runtime_schema, &.{.{ .key = "\x00\x00__metadata__:schema_json", .value = after_json }});
    defer prepared.deinit();
    var after_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(prepared.integrity_catalog.?.value, &after_digest, .{});
    var schema_json_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(after_json, &schema_json_digest, .{});
    var before_schema_json_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(before_json, &before_schema_json_digest, .{});
    const topology = @import("relational_integrity_topology.zig");
    const initial_fence: topology.Fence = .{ .role = .child_generation_source, .transition_id = 11, .attempt = 1, .admission_epoch = 1, .peer_group_id = 41, .owner_group_id = 41, .namespace = namespace, .catalog_digest = before_digest };
    try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = initial_fence }, null);
    try std.testing.expectError(error.IntegrityTopologyBusy, db.setSchemaJson(alloc, after_json));
    try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .action = .release, .fence = initial_fence }, null));
    try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .action = .cancel, .fence = initial_fence }, null));
    try db.applyRelationalTopologyControl(.{ .action = .cancel_child_generation_source, .fence = initial_fence }, null);
    var fence = initial_fence;
    fence.transition_id = 12;
    fence.admission_epoch = 2;
    try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = fence }, null);
    const publication: db_mod.DB.PublishedChildSchema = .{ .fence = fence, .before_schema_json_digest = before_schema_json_digest, .schema_json_digest = schema_json_digest, .before_catalog_digest = before_digest, .after_catalog_digest = after_digest, .raft_entry = .{ .term = 1, .index = 1 } };
    var changed = publication;
    changed.after_catalog_digest = @splat(9);
    try std.testing.expectError(error.IntegrityCatalogChanged, db.installPublishedChildSchema(alloc, after_json, changed));
    changed = publication;
    changed.before_schema_json_digest = @splat(9);
    try std.testing.expectError(error.IntegrityCatalogChanged, db.installPublishedChildSchema(alloc, after_json, changed));
    const command: topology.Command = .{ .action = .install_child_schema, .fence = fence, .child_schema_install = .{ .schema_json = after_json, .before_schema_json_digest = before_schema_json_digest, .schema_json_digest = schema_json_digest, .before_catalog_digest = before_digest, .after_catalog_digest = after_digest } };
    try db.batchRaftReplicatedApply(.{ .relational_topology = command }, publication.raft_entry);
    try db.batchRaftReplicatedApply(.{ .relational_topology = command }, publication.raft_entry);
    const installed = (try db.getSchemaJson(alloc)).?;
    defer alloc.free(installed);
    try std.testing.expectEqualStrings(after_json, installed);
    const new_catalog = try db.core.store.get(alloc, catalog.key);
    defer alloc.free(new_catalog);
    var actual_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(new_catalog, &actual_digest, .{});
    try std.testing.expectEqualSlices(u8, &after_digest, &actual_digest);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try topology.current(&read) == null);
    try std.testing.expect((try topology.completed(&read)).?.eql(fence));

    // A hot standby replays the dedicated schema-cut event after the source
    // begin batch. A generic metadata event would reject the FK generation
    // change or lose the source release on promotion.
    const standby_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-generation-child-standby", .{tmp.sub_path});
    defer alloc.free(standby_path);
    var standby = try db_mod.DB.open(alloc, standby_path, .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false });
    var standby_open = true;
    defer if (standby_open) standby.close();
    try standby.setSchemaJson(alloc, before_json);
    try standby.applyRelationalTopologyControl(.{ .action = .begin, .fence = fence }, null);
    const effects = @import("../hot_standby/effects.zig");
    const bad_payload = try effects.encodePublishedChildSchemaMetadataMutationAlloc(alloc, runtime_schema, after_json, .{
        .fence = fence,
        .before_schema_json_digest = before_schema_json_digest,
        .schema_json_digest = schema_json_digest,
        .before_catalog_digest = before_digest,
        .after_catalog_digest = @splat(9),
        .applied_term = publication.raft_entry.term,
        .applied_index = publication.raft_entry.index,
    });
    defer alloc.free(bad_payload);
    const bad_record: @import("../hot_standby/replication_record.zig").Record = .{
        .kind = .metadata_mutation,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = bad_payload,
    };
    try std.testing.expectError(error.IntegrityCatalogChanged, standby.applyHAReplicationRecord(bad_record));
    const payload = try effects.encodePublishedChildSchemaMetadataMutationAlloc(alloc, runtime_schema, after_json, .{
        .fence = fence,
        .before_schema_json_digest = before_schema_json_digest,
        .schema_json_digest = schema_json_digest,
        .before_catalog_digest = before_digest,
        .after_catalog_digest = after_digest,
        .applied_term = publication.raft_entry.term,
        .applied_index = publication.raft_entry.index,
    });
    defer alloc.free(payload);
    const record: @import("../hot_standby/replication_record.zig").Record = .{
        .kind = .metadata_mutation,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = payload,
    };
    try standby.applyHAReplicationRecord(record);
    try standby.applyHAReplicationRecord(record);
    const standby_schema = (try standby.getSchemaJson(alloc)).?;
    defer alloc.free(standby_schema);
    try std.testing.expectEqualStrings(after_json, standby_schema);
    const standby_catalog = try standby.core.store.get(alloc, catalog.key);
    defer alloc.free(standby_catalog);
    try std.testing.expectEqualSlices(u8, new_catalog, standby_catalog);
    var standby_read = try standby.core.store.beginReadTxn();
    try std.testing.expect(try topology.current(&standby_read) == null);
    try std.testing.expect((try topology.completed(&standby_read)).?.eql(fence));
    standby_read.abort();
    const receipt_key = @import("relational_integrity_generation_admission.zig").source_install_receipt_key;
    const primary_receipt = try db.core.store.get(alloc, receipt_key);
    defer alloc.free(primary_receipt);
    const standby_receipt = try standby.core.store.get(alloc, receipt_key);
    defer alloc.free(standby_receipt);
    try std.testing.expectEqualSlices(u8, primary_receipt, standby_receipt);
    standby.close();
    standby_open = false;
    // Promotion/restart must load the same durable schema and receipt without
    // consulting the old metadata decision or replaying the journal event.
    var promoted = try db_mod.DB.open(alloc, standby_path, .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false });
    defer promoted.close();
    const promoted_schema = (try promoted.getSchemaJson(alloc)).?;
    defer alloc.free(promoted_schema);
    try std.testing.expectEqualStrings(after_json, promoted_schema);
    const promoted_catalog = try promoted.core.store.get(alloc, catalog.key);
    defer alloc.free(promoted_catalog);
    try std.testing.expectEqualSlices(u8, new_catalog, promoted_catalog);
    const promoted_receipt = try promoted.core.store.get(alloc, receipt_key);
    defer alloc.free(promoted_receipt);
    try std.testing.expectEqualSlices(u8, primary_receipt, promoted_receipt);
    var promoted_read = try promoted.core.store.beginReadTxn();
    defer promoted_read.abort();
    try std.testing.expect(try topology.current(&promoted_read) == null);
    try std.testing.expect((try topology.completed(&promoted_read)).?.eql(fence));
}

test "initial FK child owner stays hidden across restart until replicated release" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-initial-child", .{tmp.sub_path});
    defer alloc.free(path);
    const standby_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-initial-child-standby", .{tmp.sub_path});
    defer alloc.free(standby_path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = std.hash.Wyhash.hash(0x54424c45, "table:11"), .shard_id = 41, .range_id = 41 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false };
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"parent_fk","child_columns":["parent_id"],"parent_table":"parents","parent_columns":["id"]},{"name":"self_fk","child_columns":["parent_id"],"parent_table":"table:11","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const schema = @import("../../schema/mod.zig");
    var parsed = try schema.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime = try schema.deriveRuntimeTableSchema(alloc, parsed);
    defer @import("../schema.zig").freeSchema(alloc, runtime);
    const hidden = @import("relational_initial_child_publication.zig");
    const topology = @import("relational_integrity_topology.zig");
    const plan_id: [16]u8 = @splat(7);
    const plan_digest: [32]u8 = @splat(8);
    var catalog_digest: [32]u8 = undefined;
    var schema_digest: [32]u8 = undefined;
    var public_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(schema_json, &public_digest, .{});
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        var candidate = try db.core.prepareSchemaMetadataPublishedChild(runtime, &.{.{ .key = "\x00\x00__metadata__:schema_json", .value = schema_json }});
        defer candidate.deinit();
        std.crypto.hash.Blake3.hash(candidate.integrity_catalog.?.value, &catalog_digest, .{});
        schema_digest = candidate.integrity_catalog.?.catalog.schema_digest;
        const fence: topology.Fence = .{ .role = .child_generation_source, .transition_id = 11, .attempt = 1, .admission_epoch = 1, .peer_group_id = 41, .owner_group_id = 41, .namespace = namespace, .catalog_digest = catalog_digest };
        const provision: topology.Command = .{ .action = .provision_initial_child, .fence = fence, .initial_child_provision = .{
            .schema_json = schema_json,
            .child_table_name = "table:11",
            .plan_id = plan_id,
            .plan_digest = plan_digest,
            .schema_digest = schema_digest,
            .public_schema_json_digest = public_digest,
            .catalog_digest = catalog_digest,
        } };
        try db.batchRaftReplicatedApply(.{ .relational_topology = provision }, .{ .term = 2, .index = 1 });
        try db.batchRaftReplicatedApply(.{ .relational_topology = provision }, .{ .term = 2, .index = 1 });
        try std.testing.expectError(error.InitialChildNotPublished, db.batch(.{ .writes = &.{.{ .key = "leak", .value = "{}" }} }));
        try std.testing.expectError(error.InitialChildNotPublished, db.lookup(alloc, "leak", .{}));
        // The release controller must be able to probe self-parent witness
        // readiness while ordinary point reads remain hidden. This fixture
        // has no secondary index, so it reaches IndexNotFound past the gate.
        try std.testing.expectError(error.IndexNotFound, db.lookup(alloc, "", .{ .relational_index_status_json = "{\"name\":\"self_witness\",\"schema_version\":1}" }));
        const stored = try db.core.store.get(alloc, hidden.key);
        defer alloc.free(stored);
        try std.testing.expectEqual(hidden.Phase.hidden, (try hidden.Record.decode(stored)).phase);
        var owner_read = try db.core.store.beginReadTxn();
        defer owner_read.abort();
        const self_scope = (try @import("relational_integrity_generation_admission.zig").load(&owner_read, "table:11", "self_fk")).?;
        try std.testing.expectEqual(@import("relational_integrity_generation_admission.zig").Phase.active, self_scope.phase);
        try std.testing.expectEqual(namespace.table_id, self_scope.child_table_id);
        try std.testing.expectEqualSlices(u8, &plan_id, &self_scope.plan_id);
        try std.testing.expectEqualSlices(u8, &plan_digest, &self_scope.decision_digest);
        const bootstrap: hidden.Bootstrap = .{
            .plan_id = plan_id,
            .plan_digest = plan_digest,
            .namespace = namespace,
            .schema_version = 1,
            .schema_digest = schema_digest,
            .public_schema_json_digest = public_digest,
            .catalog_digest = catalog_digest,
        };
        var standby_options = options;
        standby_options.initial_child_bootstrap = bootstrap;
        var standby = try db_mod.DB.open(alloc, standby_path, standby_options);
        defer standby.close();
        const payload = try @import("../hot_standby/effects.zig").encodeInitialChildMutationRequestAlloc(alloc, .{ .relational_topology = provision }, .{ .term = 2, .index = 1 });
        defer alloc.free(payload);
        const ha_record: @import("../hot_standby/replication_record.zig").RecordView = .{
            .kind = .batch_mutation,
            .payload_codec = .json,
            .cluster_id = 1,
            .timeline_id = 1,
            .epoch = 1,
            .lsn = 1,
            .previous_lsn = 0,
            .payload = payload,
        };
        try standby.applyHAReplicationRecord(ha_record);
        try standby.applyHAReplicationRecord(ha_record);
        try std.testing.expectError(error.InitialChildNotPublished, standby.lookup(alloc, "leak", .{}));
        const replayed = try standby.core.store.get(alloc, hidden.key);
        defer alloc.free(replayed);
        try std.testing.expectEqualSlices(u8, stored, replayed);
        var standby_read = try standby.core.store.beginReadTxn();
        defer standby_read.abort();
        const replayed_scope = (try @import("relational_integrity_generation_admission.zig").load(&standby_read, "table:11", "self_fk")).?;
        try std.testing.expectEqualSlices(u8, &self_scope.active_generation.?, &replayed_scope.active_generation.?);
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expectError(error.InitialChildNotPublished, db.batch(.{ .writes = &.{.{ .key = "leak", .value = "{}" }} }));
        var restart_read = try db.core.store.beginReadTxn();
        defer restart_read.abort();
        try std.testing.expect((try @import("relational_integrity_generation_admission.zig").load(&restart_read, "table:11", "self_fk")) != null);
        const fence: topology.Fence = .{ .role = .child_generation_source, .transition_id = 11, .attempt = 1, .admission_epoch = 1, .peer_group_id = 41, .owner_group_id = 41, .namespace = namespace, .catalog_digest = catalog_digest };
        const release: topology.Command = .{ .action = .release_initial_child, .fence = fence, .initial_child_control = .{
            .plan_id = plan_id,
            .plan_digest = plan_digest,
            .schema_version = 1,
            .schema_digest = schema_digest,
            .public_schema_json_digest = public_digest,
            .catalog_digest = catalog_digest,
        } };
        var wrong = release;
        wrong.initial_child_control.?.plan_digest = @splat(9);
        try std.testing.expectError(error.InitialChildPublicationChanged, db.batchRaftReplicatedApply(.{ .relational_topology = wrong }, .{ .term = 2, .index = 2 }));
        const self_key = try @import("relational_integrity_generation_admission.zig").scopeKey("table:11", "self_fk");
        const self_scope_bytes = try db.core.store.get(alloc, &self_key);
        defer alloc.free(self_scope_bytes);
        var corrupted = try db.core.store.beginWriteTxn();
        try corrupted.delete(&self_key);
        try corrupted.commit();
        try std.testing.expectError(error.InitialChildPublicationChanged, db.batchRaftReplicatedApply(.{ .relational_topology = release }, .{ .term = 2, .index = 2 }));
        var repaired = try db.core.store.beginWriteTxn();
        try repaired.put(&self_key, self_scope_bytes);
        try repaired.commit();
        try db.batchRaftReplicatedApply(.{ .relational_topology = release }, .{ .term = 2, .index = 2 });
        try db.batchRaftReplicatedApply(.{ .relational_topology = release }, .{ .term = 2, .index = 2 });
        const stored = try db.core.store.get(alloc, hidden.key);
        defer alloc.free(stored);
        try std.testing.expectEqual(hidden.Phase.released, (try hidden.Record.decode(stored)).phase);
        try std.testing.expect((try db.lookup(alloc, "leak", .{})) == null);
        try std.testing.expectError(error.InvalidIntegrityOperation, db.batch(.{ .writes = &.{.{ .key = hidden.key, .value = "forged" }} }));
        var standby_options = options;
        standby_options.initial_child_bootstrap = .{
            .plan_id = plan_id,
            .plan_digest = plan_digest,
            .namespace = namespace,
            .schema_version = 1,
            .schema_digest = schema_digest,
            .public_schema_json_digest = public_digest,
            .catalog_digest = catalog_digest,
        };
        // The standby was offline when the primary committed release. A
        // promotion before catch-up must retain its durable hidden gate;
        // local restart or a stale bootstrap is not publication authority.
        {
            var offline_promoted = try db_mod.DB.open(alloc, standby_path, standby_options);
            defer offline_promoted.close();
            try std.testing.expectError(error.InitialChildNotPublished, offline_promoted.lookup(alloc, "leak", .{}));
            try std.testing.expectError(error.InitialChildNotPublished, offline_promoted.batch(.{ .writes = &.{.{ .key = "leak", .value = "{}" }} }));
            const hidden_bytes = try offline_promoted.core.store.get(alloc, hidden.key);
            defer alloc.free(hidden_bytes);
            try std.testing.expectEqual(hidden.Phase.hidden, (try hidden.Record.decode(hidden_bytes)).phase);
        }
        var standby = try db_mod.DB.open(alloc, standby_path, standby_options);
        var standby_open = true;
        defer if (standby_open) standby.close();
        const payload = try @import("../hot_standby/effects.zig").encodeInitialChildMutationRequestAlloc(alloc, .{ .relational_topology = release }, .{ .term = 2, .index = 2 });
        defer alloc.free(payload);
        const record: @import("../hot_standby/replication_record.zig").RecordView = .{
            .kind = .batch_mutation,
            .payload_codec = .json,
            .cluster_id = 1,
            .timeline_id = 1,
            .epoch = 1,
            .lsn = 2,
            .previous_lsn = 1,
            .payload = payload,
        };
        try standby.applyHAReplicationRecord(record);
        try standby.applyHAReplicationRecord(record);
        const replayed = try standby.core.store.get(alloc, hidden.key);
        defer alloc.free(replayed);
        try std.testing.expectEqualSlices(u8, stored, replayed);
        try std.testing.expect((try standby.lookup(alloc, "leak", .{})) == null);
        standby.close();
        standby_open = false;
        var caught_up_promoted = try db_mod.DB.open(alloc, standby_path, standby_options);
        defer caught_up_promoted.close();
        try std.testing.expect((try caught_up_promoted.lookup(alloc, "leak", .{})) == null);
        const promoted_bytes = try caught_up_promoted.core.store.get(alloc, hidden.key);
        defer alloc.free(promoted_bytes);
        try std.testing.expectEqual(hidden.Phase.released, (try hidden.Record.decode(promoted_bytes)).phase);
    }
}

test "initial FK bootstrap denies direct writes before first Raft provision and survives reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-bootstrap", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 51, .shard_id = 41, .range_id = 41 };
    const bootstrap: @import("relational_initial_child_publication.zig").Bootstrap = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .namespace = namespace,
        .schema_version = 0,
        .schema_digest = @splat(3),
        .public_schema_json_digest = @splat(4),
        .catalog_digest = @splat(5),
    };
    const options: db_mod.OpenOptions = .{
        .identity_namespace = namespace,
        .initial_child_bootstrap = bootstrap,
        .start_optional_runtimes = false,
        .start_index_workers = false,
    };
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expectError(error.InitialChildNotPublished, db.batch(.{ .writes = &.{.{ .key = "leak", .value = "{}" }} }));
        try std.testing.expectError(error.InitialChildNotPublished, db.lookup(alloc, "leak", .{}));
        try std.testing.expectError(error.InitialChildNotPublished, db.scan(alloc, "", "", .{}));
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expectError(error.InitialChildNotPublished, db.batch(.{ .writes = &.{.{ .key = "leak", .value = "{}" }} }));
    }
}

test "parent FK generation owner stages default-deny, activates atomically, and fences until metadata ACK" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-generation-parent", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 41, .shard_id = 31, .range_id = 31 };
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    const raw = try db.core.store.get(alloc, catalog.key);
    defer alloc.free(raw);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(raw, &digest, .{});
    const topology = @import("relational_integrity_topology.zig");
    const admission = @import("relational_integrity_generation_admission.zig");
    const transition: admission.Transition = .{ .child_table_id = 51, .child_table_name = "children", .constraint_name = "fk", .expected_generation = null, .next_generation = @splat(5), .plan_id = @splat(4), .decision_digest = @splat(6) };
    var second_constraint = transition;
    second_constraint.constraint_name = "fk2";
    second_constraint.next_generation = @splat(7);
    const transitions = [_]admission.Transition{ transition, second_constraint };
    const first: topology.Fence = .{ .role = .child_generation_parent, .transition_id = 11, .attempt = 1, .admission_epoch = 1, .peer_group_id = 51, .owner_group_id = 31, .namespace = namespace, .catalog_digest = digest };
    try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = first }, null);
    try db.applyRelationalTopologyControl(.{ .action = .stage_child_generation, .fence = first, .child_generations = &transitions }, null);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const scope = (try admission.load(&read, transition.child_table_name, transition.constraint_name)).?;
        try std.testing.expectEqual(admission.Phase.staged, scope.phase);
        try std.testing.expect(scope.active_generation == null);
        const second_scope = (try admission.load(&read, second_constraint.child_table_name, second_constraint.constraint_name)).?;
        try std.testing.expectEqual(admission.Phase.staged, second_scope.phase);
    }
    try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .action = .release, .fence = first }, null));
    try db.applyRelationalTopologyControl(.{ .action = .cancel, .fence = first, .child_generations = &transitions }, null);
    var second = first;
    second.transition_id = 12;
    second.admission_epoch = 2;
    try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = second }, null);
    try db.applyRelationalTopologyControl(.{ .action = .stage_child_generation, .fence = second, .child_generations = &transitions }, null);
    try db.applyRelationalTopologyControl(.{ .action = .activate_child_generation, .fence = second, .child_generations = &transitions }, null);
    try db.applyRelationalTopologyControl(.{ .action = .activate_child_generation, .fence = second, .child_generations = &transitions }, null);
    try std.testing.expectError(error.GenerationAdmissionChanged, db.applyRelationalTopologyControl(.{ .action = .cancel, .fence = second, .child_generations = &transitions }, null));
    var third = second;
    third.transition_id = 13;
    third.admission_epoch = 3;
    try std.testing.expectError(error.GenerationAdmissionAcknowledgementPending, db.applyRelationalTopologyControl(.{ .action = .begin, .fence = third }, null));
    try db.applyRelationalTopologyControl(.{ .action = .acknowledge_child_generation, .fence = second, .child_generations = &transitions }, null);
    try db.applyRelationalTopologyControl(.{ .action = .acknowledge_child_generation, .fence = second, .child_generations = &transitions }, null);
    try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = third }, null);
    var next_transition = transition;
    next_transition.expected_generation = transition.next_generation;
    next_transition.next_generation = @splat(7);
    next_transition.plan_id = @splat(8);
    next_transition.decision_digest = @splat(9);
    try db.applyRelationalTopologyControl(.{ .action = .cancel, .fence = third, .child_generations = &.{next_transition} }, null);
    const retirement = @import("relational_integrity_generation_retirement.zig");
    var fourth = third;
    fourth.transition_id = 14;
    fourth.admission_epoch = 4;
    try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = fourth }, null);
    next_transition.next_generation = null; // DROP CONSTRAINT
    try db.applyRelationalTopologyControl(.{ .action = .stage_child_generation, .fence = fourth, .child_generations = &.{next_transition} }, null);
    try db.applyRelationalTopologyControl(.{ .action = .activate_child_generation, .fence = fourth, .child_generations = &.{next_transition} }, null);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const scope = (try admission.load(&read, transition.child_table_name, transition.constraint_name)).?;
        try std.testing.expect(scope.active_generation == null);
        const active = try read.get(&retirement.activeKey(@splat(5)));
        _ = try retirement.Active.decode(active, @splat(5));
        const progress = try read.get(retirement.gc_progress_key);
        try std.testing.expect(!(try retirement.GcProgress.decode(progress)).complete);
    }
    try db.applyRelationalTopologyControl(.{ .action = .acknowledge_child_generation, .fence = fourth, .child_generations = &.{next_transition} }, null);
    const scope_key = try admission.scopeKey("children", "fk");
    try std.testing.expectError(error.InvalidIntegrityOperation, db.batch(.{ .writes = &.{.{ .key = &scope_key, .value = "forged" }} }));
    try std.testing.expectError(error.InvalidIntegrityOperation, db.batch(.{ .deletes = &.{admission.source_install_receipt_key} }));
    var fifth = fourth;
    fifth.transition_id = 15;
    fifth.admission_epoch = 5;
    try db.applyRelationalTopologyControl(.{ .action = .begin, .fence = fifth }, null);
    var reparent = second_constraint;
    reparent.expected_generation = second_constraint.next_generation;
    reparent.next_generation = @splat(8);
    reparent.plan_id = @splat(10);
    reparent.decision_digest = @splat(11);
    try db.applyRelationalTopologyControl(.{ .action = .stage_child_generation, .fence = fifth, .child_generations = &.{reparent} }, null);
    try db.applyRelationalTopologyControl(.{ .action = .activate_child_generation, .fence = fifth, .child_generations = &.{reparent} }, null);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const active = try read.get(&retirement.activeKey(@splat(7)));
        _ = try retirement.Active.decode(active, @splat(7));
    }
}

test "relational integrity TRUNCATE parent pending generations survive restart and reject changed replay" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/truncate-parent", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 41, .shard_id = 31, .range_id = 31 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false };
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const topology = @import("relational_integrity_topology.zig");
    const retirement = @import("relational_integrity_generation_retirement.zig");
    const entry: @import("relational_integrity_topology_contract.zig").ParentRetirementEntry = .{ .child_table_id = 51, .child_table_name = "children", .constraint_name = "fk", .generation = @splat(3), .next_generation = @splat(4) };
    var fence: topology.Fence = .{ .role = .truncate_parent, .transition_id = 11, .attempt = 1, .peer_group_id = 21, .owner_group_id = 31, .namespace = namespace, .catalog_digest = undefined };
    const stage: @import("relational_integrity_topology_contract.zig").ParentRetirementStage = .{ .plan_digest = @splat(5), .entries = &.{entry} };
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, schema);
        const raw = try db.core.store.get(alloc, catalog.key);
        defer alloc.free(raw);
        std.crypto.hash.Blake3.hash(raw, &fence.catalog_digest, .{});
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null);
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = stage }, null);
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = stage }, null);
        try std.testing.expectError(error.GenerationRetirementChanged, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = .{ .plan_digest = @splat(6), .entries = stage.entries } }, null));
        try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .release }, null));
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = stage }, null);
        const changed: @import("relational_integrity_topology_contract.zig").ParentRetirementEntry = .{ .child_table_id = entry.child_table_id, .child_table_name = entry.child_table_name, .constraint_name = entry.constraint_name, .generation = @splat(5), .next_generation = entry.next_generation };
        try std.testing.expectError(error.GenerationRetirementChanged, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = .{ .plan_digest = stage.plan_digest, .entries = &.{changed} } }, null));
        var renamed = entry;
        renamed.child_table_name = "renamed_children";
        try std.testing.expectError(error.GenerationRetirementChanged, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = .{ .plan_digest = stage.plan_digest, .entries = &.{renamed} } }, null));
        renamed = entry;
        renamed.constraint_name = "renamed_fk";
        try std.testing.expectError(error.GenerationRetirementChanged, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = .{ .plan_digest = stage.plan_digest, .entries = &.{renamed} } }, null));
        try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .release }, null));
        const pending = try db.core.store.get(alloc, retirement.key);
        defer alloc.free(pending);
        try std.testing.expect((try retirement.Pending.decode(pending)).containsTransition(entry.child_table_id, entry.generation, entry.next_generation));
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .cancel }, null);
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, retirement.key));
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .cancel }, null);
        try std.testing.expectError(error.IntegrityTopologyCompleted, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, retirement.key));
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try topology.completed(&read)).?.eql(fence));
    }
}

test "relational integrity accepted generation survives restart and bounded two-phase GC never resurrects references" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/retired-parent", .{tmp.sub_path});
    defer alloc.free(path);
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 41, .shard_id = 31, .range_id = 31 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false };
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const topology = @import("relational_integrity_topology.zig");
    const retirement = @import("relational_integrity_generation_retirement.zig");
    const address = try integrity.Address.init(@splat(7), "parent-tuple");
    const claim: integrity.Claim = .{ .tuple = "parent-tuple", .parent_table = "parents", .parent_key = "p", .schema_version = 1 };
    const old: integrity.Reference = .{ .child_table = "children", .child_key = "old", .constraint_name = "fk", .constraint_generation = @splat(3) };
    var live: integrity.Reference = .{ .child_table = "children", .child_key = "live", .constraint_name = "other_fk", .constraint_generation = @splat(4) };
    const old_key = try old.key(address);
    var live_key = try live.key(address);
    var live_key_buffer: [32]u8 = undefined;
    var salt: usize = 0;
    while (std.mem.order(u8, &old_key, &live_key) != .lt) : (salt += 1) {
        if (salt >= 256) return error.TestNoOrderedReferenceKey;
        live.child_key = try std.fmt.bufPrint(&live_key_buffer, "live-{d}", .{salt});
        live_key = try live.key(address);
    }
    var fence: topology.Fence = .{ .role = .truncate_parent, .transition_id = 11, .attempt = 1, .peer_group_id = 21, .owner_group_id = 31, .namespace = namespace, .catalog_digest = undefined };
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, schema);
        const raw = try db.core.store.get(alloc, catalog.key);
        defer alloc.free(raw);
        std.crypto.hash.Blake3.hash(raw, &fence.catalog_digest, .{});
        const claim_bytes = try claim.encode(alloc, address);
        defer alloc.free(claim_bytes);
        const old_bytes = try old.encode(alloc, address);
        defer alloc.free(old_bytes);
        const live_bytes = try live.encode(alloc, address);
        defer alloc.free(live_bytes);
        try db.core.store.put(&address.claimKey(), claim_bytes);
        try db.core.store.put(&old_key, old_bytes);
        try db.core.store.put(&live_key, live_bytes);
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null);
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = .{ .plan_digest = @splat(5), .entries = &.{.{ .child_table_id = 51, .child_table_name = "children", .constraint_name = "fk", .generation = old.constraint_generation, .next_generation = @splat(5) }} } }, null);
        const plan_id: [16]u8 = @splat(4);
        const publication_digest = retirement.publicationDigest(plan_id, @splat(5));
        const activate: topology.Command = .{ .fence = fence, .action = .activate_parent_retirement, .parent_activation = .{ .plan_id = plan_id, .plan_digest = @splat(5), .publication_digest = publication_digest } };
        try db.applyRelationalTopologyControl(activate, null);
        try db.applyRelationalTopologyControl(activate, null);
        {
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            try std.testing.expect((try topology.current(&probe)).?.eql(fence));
            try std.testing.expectError(error.IntegrityTopologyBusy, topology.requireUnfenced(&probe));
        }
        try std.testing.expectError(error.GenerationRetirementChanged, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .activate_parent_retirement, .parent_activation = .{ .plan_id = @splat(6), .plan_digest = @splat(5), .publication_digest = retirement.publicationDigest(@splat(6), @splat(5)) } }, null));
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        const replay_stage: topology.Command = .{ .fence = fence, .action = .stage_parent_retirement, .parent_retirement = .{ .plan_digest = @splat(5), .entries = &.{.{ .child_table_id = 51, .child_table_name = "children", .constraint_name = "fk", .generation = old.constraint_generation, .next_generation = @splat(5) }} } };
        try db.applyRelationalTopologyControl(replay_stage, null);
        var changed_stage = replay_stage;
        changed_stage.parent_retirement = .{ .plan_digest = @splat(6), .entries = replay_stage.parent_retirement.?.entries };
        try std.testing.expectError(error.GenerationRetirementChanged, db.applyRelationalTopologyControl(changed_stage, null));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, retirement.key));
        {
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            const status = (try retirement.ownerStatus(alloc, &probe)).?;
            defer alloc.free(status.entries);
            try std.testing.expect(status.completed);
            try std.testing.expect(!status.acknowledged);
        }
        var split = fence;
        split.role = .split_source;
        split.transition_id += 1;
        split.admission_epoch += 1;
        try std.testing.expectError(error.IntegrityTopologyBusy, db.applyRelationalTopologyControl(.{ .fence = split, .action = .begin }, null));
        try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .release }, null));
        try std.testing.expectError(error.GenerationAdmissionActivationRequired, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .cancel }, null));
        const gc_before_ack = try db.core.store.get(alloc, retirement.gc_progress_key);
        defer alloc.free(gc_before_ack);
        const previous_gc = try retirement.GcProgress.decode(gc_before_ack);
        const tombstone_phase = try (retirement.GcProgress{ .revision = previous_gc.revision, .tombstones = true }).encode(alloc);
        defer alloc.free(tombstone_phase);
        try db.core.store.put(retirement.gc_progress_key, tombstone_phase);
        {
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            try std.testing.expect((try retirement.prepareGcPage(alloc, &probe, 1, 1024 * 1024)) == null);
        }
        try db.core.store.put(retirement.gc_progress_key, gc_before_ack);
        const plan_id: [16]u8 = @splat(4);
        const acknowledge: topology.Command = .{ .fence = fence, .action = .acknowledge_parent_retirement, .parent_activation = .{ .plan_id = plan_id, .plan_digest = @splat(5), .publication_digest = retirement.publicationDigest(plan_id, @splat(5)) } };
        try std.testing.expectError(error.GenerationRetirementChanged, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .acknowledge_parent_retirement, .parent_activation = .{ .plan_id = @splat(6), .plan_digest = @splat(5), .publication_digest = retirement.publicationDigest(@splat(6), @splat(5)) } }, null));
        try db.applyRelationalTopologyControl(acknowledge, null);
        try db.applyRelationalTopologyControl(acknowledge, null);
        {
            var probe = try db.core.store.beginProbeTxn();
            defer probe.abort();
            try std.testing.expect((try topology.current(&probe)) == null);
            try topology.requireUnfenced(&probe);
        }
        try db.applyRelationalTopologyControl(.{ .fence = split, .action = .begin }, null);
        try db.applyRelationalTopologyControl(acknowledge, null);
        try db.applyRelationalTopologyControl(.{ .fence = split, .action = .cancel }, null);
        var read = try integrity.CurrentView.init(db.core.store);
        var read_open = true;
        defer if (read_open) read.deinit();
        try std.testing.expect(try retirement.isRetired(&read, old));
        try std.testing.expect(!(try retirement.isRetired(&read, live)));
        const accepted = (try @import("relational_integrity_generation_admission.zig").load(&read, old.child_table, old.constraint_name)).?;
        try std.testing.expectEqual(@as(integrity.Generation, @splat(5)), accepted.active_generation.?);
        var unknown_generation = old;
        unknown_generation.constraint_generation = @splat(9);
        try std.testing.expect(try retirement.isRetired(&read, unknown_generation));
        var successor = old;
        successor.constraint_generation = @splat(5);
        try std.testing.expect(!(try retirement.isRetired(&read, successor)));
        try std.testing.expectError(error.GenerationRetired, integrity.prepare(alloc, &read, &.{.{ .address = address, .operation = .{ .attach = old } }}));
        try std.testing.expectError(error.ForeignKeyReferenced, integrity.prepare(alloc, &read, &.{.{ .address = address, .operation = .{ .release = .{ .parent_table = "parents", .parent_key = "p" } } }}));
        // The public parent-action cursor must not revive retired references.
        // With a one-record physical page, the first page is empty but still
        // advances; the live successor appears on the next page.
        const query = try std.json.Stringify.valueAlloc(alloc, .{ .kind = "references", .address = address, .limit = 1 }, .{});
        defer alloc.free(query);
        var first = (try db.lookup(alloc, &address.routing, .{ .relational_integrity_jobs_json = query })) orelse return error.TestMissingReferencePage;
        defer first.deinit(alloc);
        var first_page = try std.json.parseFromSlice(struct { references: []std.json.Value, next: ?[]const u8 }, alloc, first.json, .{ .ignore_unknown_fields = true });
        defer first_page.deinit();
        try std.testing.expectEqual(@as(usize, 0), first_page.value.references.len);
        const continuation = first_page.value.next orelse return error.TestMissingReferenceContinuation;
        const resumed_query = try std.json.Stringify.valueAlloc(alloc, .{ .kind = "references", .address = address, .after = continuation, .limit = 1 }, .{});
        defer alloc.free(resumed_query);
        var second = (try db.lookup(alloc, &address.routing, .{ .relational_integrity_jobs_json = resumed_query })) orelse return error.TestMissingReferencePage;
        defer second.deinit(alloc);
        var second_page = try std.json.parseFromSlice(struct { references: []std.json.Value, next: ?[]const u8 }, alloc, second.json, .{ .ignore_unknown_fields = true });
        defer second_page.deinit();
        try std.testing.expectEqual(@as(usize, 1), second_page.value.references.len);
        try std.testing.expect(second_page.value.next == null);
        // Exercise the owner lookup used by the supervisor, not just the GC
        // helper: a point-probe transaction cannot provide its range cursor.
        var gc_lookup = (try db.lookup(alloc, &address.routing, .{ .relational_topology_json = "{\"mode\":\"generation_gc\"}" })) orelse return error.TestMissingGenerationGcPage;
        defer gc_lookup.deinit(alloc);
        try std.testing.expect(!std.mem.eql(u8, gc_lookup.json, "null"));
        // Exactly one reference is examined per page, including live rows;
        // each committed progress record survives an owner reopen.
        var pages: usize = 0;
        while (pages < 6) : (pages += 1) {
            var page = (try retirement.prepareGcPage(alloc, &read, 1, 1024 * 1024)) orelse break;
            defer page.deinit();
            const command = page.command(namespace.shard_id, namespace);
            read.deinit();
            read_open = false;
            try db.batchRaftReplicatedApply(.{ .relational_generation_gc = command }, .{ .term = 1, .index = @intCast(pages * 2 + 1) });
            // Raft replay is idempotent, whereas a fresh stale proposal must
            // not silently advance the progress CAS or delete a new value.
            try db.batchRaftReplicatedApply(.{ .relational_generation_gc = command }, .{ .term = 1, .index = @intCast(pages * 2 + 1) });
            const progress_before_stale = try db.core.store.get(alloc, retirement.gc_progress_key);
            defer alloc.free(progress_before_stale);
            try db.batchRaftReplicatedApply(.{ .relational_generation_gc = command }, .{ .term = 1, .index = @intCast(pages * 2 + 2) });
            const progress_after_stale = try db.core.store.get(alloc, retirement.gc_progress_key);
            defer alloc.free(progress_after_stale);
            try std.testing.expectEqualSlices(u8, progress_before_stale, progress_after_stale);
            read = try integrity.CurrentView.init(db.core.store);
            read_open = true;
            if ((try retirement.GcProgress.decode(try read.get(retirement.gc_progress_key))).complete) break;
        }
        try std.testing.expect(pages < 6);
        try std.testing.expect((try retirement.GcProgress.decode(try read.get(retirement.gc_progress_key))).complete);
        try std.testing.expectError(error.NotFound, read.get(&old_key));
        try std.testing.expectError(error.NotFound, read.get(&retirement.activeKey(old.constraint_generation)));
        _ = try integrity.Reference.decode(&live_key, try read.get(&live_key));
        try std.testing.expect(try retirement.isRetired(&read, old));
        try std.testing.expectError(error.GenerationRetired, integrity.prepare(alloc, &read, &.{.{ .address = address, .operation = .{ .attach = old } }}));
        read.deinit();
        read_open = false;
        // A coherent native seed carries both the durable retired-generation
        // authority and the exact metadata-acknowledged owner fence. A normal
        // historical restore remains prohibited for coordinated constraints.
        _ = try db.snapshot("retired-generation");
        const snapshot_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/retired-generation", .{path});
        defer alloc.free(snapshot_path);
        const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/retired-parent-seed", .{tmp.sub_path});
        defer alloc.free(target_path);
        var transition = try @import("generation_lifecycle.zig").beginProcessExclusiveWithRuntime(target_path, null);
        defer transition.deinit();
        var staged = try transition.beginStaging();
        defer staged.deinit();
        try std.testing.expectError(error.CoordinatedConstraintRestoreRequired, db_mod.DB.restoreSnapshotToStagedGeneration(&staged, alloc, snapshot_path, staged.path(), options));
        try db_mod.DB.restoreCoherentHASeedReplicaToStagedGeneration(&staged, alloc, snapshot_path, staged.path(), options, namespace);
        var staged_options = options;
        staged_options.staged_generation = &staged;
        var seeded = try db_mod.DB.open(alloc, staged.path(), staged_options);
        defer seeded.close();
        var seeded_read = try integrity.CurrentView.init(seeded.core.store);
        defer seeded_read.deinit();
        try std.testing.expect(try retirement.isRetired(&seeded_read, old));
        const seeded_scope = (try @import("relational_integrity_generation_admission.zig").load(&seeded_read, old.child_table, old.constraint_name)).?;
        try std.testing.expectEqual(@as(integrity.Generation, @splat(5)), seeded_scope.active_generation.?);
        try retirement.requireActivationAcknowledged(&seeded_read);
        try std.testing.expectError(error.GenerationRetired, integrity.prepare(alloc, &seeded_read, &.{.{ .address = address, .operation = .{ .attach = old } }}));
    }
}

fn applyRestoreReplica(db: *db_mod.DB, request: @import("types.zig").BatchRequest, index: u64, ha: bool) !void {
    if (!ha) return db.batchRaftReplicatedApply(request, .{ .term = 1, .index = index });
    const alloc = std.testing.allocator;
    const payload = try @import("../hot_standby/effects.zig").encodeBatchMutationRequestAlloc(alloc, request);
    defer alloc.free(payload);
    try db.applyHAReplicationRecord(.{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = index, .previous_lsn = index - 1, .payload = payload });
}

test "relational integrity restore follower repairs projection and CHECK debt before validated receipt" {
    const alloc = std.testing.allocator;
    const row_count = 600;
    const restore = @import("restore_staging.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"text":{"type":"string"}},"additionalProperties":false}}}}
    ;
    const source_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/lag-source", .{tmp.sub_path});
    var source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_optional_runtimes = false, .start_index_workers = false };
    {
        var source = try db_mod.DB.open(alloc, source_path, source_options);
        defer source.close();
        try source.setSchemaJson(alloc, schema);
        var rows: [row_count]@import("types.zig").BatchWrite = undefined;
        for (&rows, 0..) |*row, n| row.* = .{ .key = try std.fmt.allocPrint(owned, "row-{d:0>4}", .{n}), .value = "{\"id\":1,\"text\":\"keyword\"}" };
        try source.batch(.{ .writes = &rows });
    }
    source_options.open_mode = .query_readonly;
    var source = try db_mod.DB.open(alloc, source_path, source_options);
    defer source.close();
    for ([_]bool{ false, true }, 0..) |ha, attempt| {
        var gate: @import("../hot_standby/public_gate_state.zig").State = .{};
        gate.role.store(@intFromEnum(@import("../hot_standby/public_gate_state.zig").Role.standby), .release);
        const path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/lag-{d}", .{ tmp.sub_path, attempt });
        const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .open_mode = .writer_no_replay, .start_optional_runtimes = false, .start_index_workers = false };
        var target = try db_mod.DB.open(alloc, path, options);
        var open = true;
        defer if (open) target.close();
        try target.setSchemaJson(alloc, schema);
        try target.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
        const encoded = try @import("../schema.zig").serializeSchema(owned, target.core.schema.?);
        const scope: restore.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = options.identity_namespace.?, .target_schema_digest = restore.digest(encoded) };
        try target.reserveRestoreStagingScoped(alloc, scope);
        if (ha) target.ha_write_gate = .{ .shared = .{ .state = &gate } };
        try applyRestoreReplica(&target, .{ .restore_staging = .{ .begin = scope } }, 1, ha);
        var index: u64 = 2;
        var crashed = false;
        while (true) : (index += 1) {
            var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 128, .none);
            defer page.deinit();
            if (page.batch) |batch| {
                {
                    db_mod.DB.failNextRestoreProjectionApplyForTest();
                    try std.testing.expectError(error.InjectedRestoreProjectionApplyFailure, applyRestoreReplica(&target, batch, index, ha));
                    target.close();
                    open = false;
                    target = try db_mod.DB.open(alloc, path, options);
                    open = true;
                    if (ha) target.ha_write_gate = .{ .shared = .{ .state = &gate } };
                    try applyRestoreReplica(&target, batch, index, ha);
                    // Model the last physical projection watermark being
                    // lost at the crash cut while the primary Raft/HA receipt
                    // remains durable. Replay must restore this local proof.
                    try @import("derived/apply_state.zig").clearAppliedSequenceWithCheckpoint(alloc, std.testing.io, target.core.store, target.core.applied_sequence_checkpoint_path, "text");
                    // Import replay is intentionally idempotent; its durable
                    // receipt cannot falsely imply projections caught up.
                    const debt = try target.listDerivedReplayDebt(alloc);
                    defer {
                        for (debt) |*entry| entry.deinit(alloc);
                        alloc.free(debt);
                    }
                    try std.testing.expect(debt.len != 0);
                    var pending = false;
                    for (debt) |entry| pending = pending or entry.catch_up_required;
                    try std.testing.expect(pending);
                    crashed = true;
                }
            }
            if (page.phase == .imported) break;
        }
        try std.testing.expect(crashed);
        // Model the coordinator's already-committed semantic CHECK coverage.
        // This fixture exercises loss of replica-local projection proof, not
        // the distributed activation protocol (covered separately below).
        {
            const activation = @import("relational_integrity_activation.zig");
            var txn = try target.core.store.beginWriteTxn();
            errdefer txn.abort();
            var compiled = try catalog.decode(alloc, try txn.get(catalog.key));
            defer compiled.deinit();
            var coverage = try activation.status(&txn, compiled);
            try std.testing.expectEqual(.check, coverage.phase);
            coverage.state = .enforced;
            coverage.rows_scanned = row_count;
            const encoded_coverage = try coverage.encode(alloc);
            defer alloc.free(encoded_coverage);
            try txn.put(activation.key, encoded_coverage);
            try txn.commit();
        }
        // CHECK coverage is disposable replica-local proof. Losing it must
        // make bounded progress on replay, never a semantic Raft rejection.
        try target.core.store.putBatch(&.{}, &.{@import("relational_constraint_jobs.zig").progress_key});
        const validate_index = index + 1;
        const validate: @import("types.zig").BatchRequest = .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .validated } } };
        try std.testing.expectError(error.RestoreProjectionCatchUpPending, applyRestoreReplica(&target, validate, validate_index, ha));
        if (ha) try std.testing.expectEqual(index, try target.haAppliedReplicationLsn()) else try std.testing.expectEqual(index, (try target.raftAppliedEntry()).?.index);
        try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "row-0000", .{}));
        var validated = false;
        // CHECK repair is time-sliced: a loaded runner can process only one
        // row per retry. Bound work by the input size plus exhaustion/receipt
        // probes, not by assuming a fixed number of rows fits into 5ms.
        for (0..row_count + 2) |_| {
            applyRestoreReplica(&target, validate, validate_index, ha) catch |err| switch (err) {
                error.RestoreProjectionCatchUpPending => continue,
                else => return err,
            };
            validated = true;
            break;
        }
        if (!validated) {
            var read = try target.core.store.beginReadTxn();
            defer read.abort();
            var view = target.core.acquireSchemaView().?;
            defer view.release();
            const status = try @import("relational_constraint_jobs.zig").status(&read, view);
            std.debug.print("restore CHECK state={s} rows={d}/{d} cursor={s}\n", .{ @tagName(status.state), status.rows_scanned, row_count, status.cursor });
            const debt = try target.listDerivedReplayDebt(alloc);
            defer {
                for (debt) |*entry| entry.deinit(alloc);
                alloc.free(debt);
            }
            std.debug.print("restore projection debt: {any}\n", .{debt});
        }
        try std.testing.expect(validated);
        try applyRestoreReplica(&target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .published } } }, validate_index + 1, ha);
        var result = try target.search(alloc, .{ .index_name = "text", .full_text = .{ .match = .{ .field = "text", .text = "keyword" } }, .limit = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, row_count), result.total_hits);
    }
}

test "relational integrity topology durable backup metadata scaling benchmark" {
    const alloc = std.testing.allocator;
    const lsm = @import("../lsm_backend.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const Case = struct { runs: usize, bytes: usize };
    for ([_]Case{ .{ .runs = 8, .bytes = 64 * 1024 }, .{ .runs = 8, .bytes = 1024 * 1024 }, .{ .runs = 64, .bytes = 64 * 1024 } }, 0..) |case, case_id| {
        const source = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/bench-{d}", .{ tmp.sub_path, case_id });
        defer alloc.free(source);
        const target = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/pin-{d}", .{ tmp.sub_path, case_id });
        defer alloc.free(target);
        var backend = try lsm.Backend.open(alloc, source, .{ .flush_threshold = 1, .compact_threshold_runs = 10000, .l0_soft_limit_runs = 10000, .l0_hard_limit_runs = 10000 });
        defer backend.close();
        const value = try alloc.alloc(u8, case.bytes);
        defer alloc.free(value);
        var random = std.Random.DefaultPrng.init(42);
        random.random().bytes(value);
        for (0..case.runs) |row| {
            var key_buf: [32]u8 = undefined;
            var txn = try backend.beginWrite();
            errdefer txn.abort();
            try txn.put(.{}, try std.fmt.bufPrint(&key_buf, "row-{d:0>6}", .{row}), value);
            try txn.commit();
        }
        const started = std.Io.Clock.awake.now(std.testing.io);
        var checkpoint = try backend.pinNativeCheckpoint();
        defer checkpoint.deinit();
        const bytes = try checkpoint.seal(std.testing.io, target, .none);
        const elapsed = started.durationTo(std.Io.Clock.awake.now(std.testing.io)).toNanoseconds();
        // File identity proves the measured path does not accidentally copy
        // corpus bytes; timings are diagnostic rather than flaky assertions.
        const linked = try std.fmt.allocPrint(alloc, "{s}/runs/{d}.tbl", .{ target, checkpoint.run_ids[0] });
        defer alloc.free(linked);
        const before = try @import("native_backup.zig").statRegularFile(std.testing.io, checkpoint.run_paths[0]);
        const after = try @import("native_backup.zig").statRegularFile(std.testing.io, linked);
        try std.testing.expectEqual(before.inode, after.inode);
        std.debug.print("\nbackup durable seal benchmark runs={} corpus_bytes={} metadata_ns={}\n", .{ checkpoint.run_ids.len, bytes, elapsed });
    }
}

test "relational integrity topology durable backup seal survives restart and later writes" {
    const alloc = std.testing.allocator;
    const seal = @import("native_backup_seal.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/seal-source", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 700, .shard_id = 701 }, .primary_backend = .{ .lsm = .{} } };
    var handle: seal.Handle = undefined;
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.batch(.{ .writes = &.{.{ .key = "before", .value = "{\"v\":1}" }} });
        const identity = try db.relationalTopologyIdentity();
        const fence: @import("relational_integrity_topology.zig").Fence = .{ .role = .backup_snapshot, .transition_id = 900, .attempt = 1, .peer_group_id = 701, .owner_group_id = 701, .admission_epoch = identity.next_epoch, .namespace = identity.namespace, .catalog_digest = identity.catalog_digest };
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null);
        const seal_start = std.Io.Clock.awake.now(std.testing.io);
        handle = try db.sealBackupCohort("attempt", fence, .none);
        std.debug.print("\nbackup seal DB fence-hold ns={}\n", .{seal_start.durationTo(std.Io.Clock.awake.now(std.testing.io)).toNanoseconds()});
        const repeated = try db.sealBackupCohort("attempt-retry", fence, .none);
        try std.testing.expectEqualSlices(u8, &handle.digest, &repeated.digest);
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .release }, null);
        try db.batch(.{ .writes = &.{.{ .key = "after", .value = "{\"v\":2}" }} });
        try db.sync(true);
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.batch(.{ .writes = &.{.{ .key = "restart", .value = "{\"v\":3}" }} });
        const Hook = struct {
            fn run(ptr: *anyopaque) !void {
                const source: *db_mod.DB = @ptrCast(@alignCast(ptr));
                // Runs inside reopened export; acquiring the normal write
                // path here would deadlock if capture admission remained held.
                try source.batch(.{ .writes = &.{.{ .key = "during-export", .value = "{\"v\":4}" }} });
            }
        };
        seal.test_export_hook = .{ .ptr = &db, .run = Hook.run };
        defer seal.test_export_hook = null;
        try std.testing.expect(try db.exportBackupCohort(handle, "export", .none) > 0);
        try std.testing.expect(try db.exportBackupCohort(handle, "export", .none) > 0);
        seal.test_export_hook = null;
        const exported_primary = try std.fmt.allocPrint(alloc, "{s}.snapshots/export/primary-lsm", .{path});
        defer alloc.free(exported_primary);
        const lsm = @import("../lsm_backend.zig");
        var image = try lsm.Backend.open(alloc, exported_primary, .{});
        defer image.close();
        var image_read = try image.beginRead();
        defer image_read.abort();
        const keys = @import("../internal_keys.zig");
        const before_key = try keys.documentKeyAlloc(alloc, "before");
        defer alloc.free(before_key);
        try std.testing.expectEqualStrings("{\"v\":1}", try image_read.get(.{ .name = "docs" }, before_key));
        for ([_][]const u8{ "after", "restart", "during-export" }) |key| {
            const physical = try keys.documentKeyAlloc(alloc, key);
            defer alloc.free(physical);
            try std.testing.expectError(error.NotFound, image_read.get(.{ .name = "docs" }, physical));
        }
        const pin = try seal.pathAlloc(alloc, path, handle.fence);
        defer alloc.free(pin);
        var opened = try seal.open(alloc, std.testing.io, pin, handle);
        defer opened.deinit();
        try std.testing.expect(opened.parsed.value.files.len > 0);
        var wrong = handle;
        wrong.digest[0] ^= 1;
        try std.testing.expectError(error.BackupSealMismatch, db.exportBackupCohort(wrong, "wrong", .none));
        // Portable and native exports must observe the same sealed cut after
        // writes resumed and the source process restarted.
        const portable = @import("../portable_backup.zig");
        var bytes = std.Io.Writer.Allocating.init(alloc);
        defer bytes.deinit();
        try db.exportBackupCohortPortable(handle, &bytes.writer, .{}, .none);
        try std.testing.expectError(error.CoordinatedConstraintPortableBackupUnsupported, portable.validatePortable(alloc, bytes.written()));
        const decoded_path = try std.fmt.allocPrint(alloc, "{s}-portable", .{path});
        defer alloc.free(decoded_path);
        var decoded = try db_mod.DB.open(alloc, decoded_path, options);
        defer decoded.close();
        try portable.importPortableWithOptions(alloc, decoded.core.store, bytes.written(), .{
            .unpublished_staging = true,
            .cohort = .{ .seal = handle, .namespace = handle.fence.namespace },
            .identity_namespace = handle.fence.namespace,
        });
        try portable.validateCompleteDatabaseImageWithCohort(alloc, decoded.core.store, .{ .seal = handle, .namespace = handle.fence.namespace });
        const before = try decoded.core.store.get(alloc, before_key);
        defer alloc.free(before);
        try std.testing.expectEqualStrings("{\"v\":1}", before);
        const after_key = try keys.documentKeyAlloc(alloc, "after");
        defer alloc.free(after_key);
        try std.testing.expectError(error.NotFound, decoded.core.store.get(alloc, after_key));
        try std.testing.expectError(error.BackupIntegrityFailure, portable.validateCompleteDatabaseImageWithCohort(alloc, decoded.core.store, .{ .seal = wrong, .namespace = handle.fence.namespace }));
        try std.testing.expectError(error.BackupIntegrityFailure, portable.importPortableWithOptions(alloc, decoded.core.store, bytes.written(), .{ .unpublished_staging = true, .cohort = .{ .seal = wrong, .namespace = handle.fence.namespace } }));
        const corrupted = try alloc.dupe(u8, bytes.written());
        defer alloc.free(corrupted);
        corrupted[@import("../backup_codec.zig").header_size + 6] ^= 1;
        try std.testing.expectError(error.BlockCrcMismatch, portable.importPortableWithOptions(alloc, decoded.core.store, corrupted, .{ .unpublished_staging = true, .cohort = .{ .seal = handle, .namespace = handle.fence.namespace } }));
        try std.testing.expectError(error.InvalidBackupRequest, portable.importPortableWithOptions(alloc, decoded.core.store, bytes.written(), .{ .cohort = .{ .seal = handle, .namespace = handle.fence.namespace } }));
        try db.releaseBackupCohort(handle);
        try db.releaseBackupCohort(handle);
        try std.testing.expectError(error.BackupSealReleased, db.exportBackupCohort(handle, "released", .none));
        try std.testing.expectError(error.BackupSealReleased, db.sealBackupCohort("delayed", handle.fence, .none));
        var canceled = handle.fence;
        canceled.transition_id += 1;
        canceled.admission_epoch += 1;
        try db.cancelBackupCohort(canceled);
        try db.cancelBackupCohort(canceled);
        try std.testing.expectError(error.BackupSealReleased, db.sealBackupCohort("never-delivered", canceled, .none));
    }
}

test "relational integrity portable decoder resumes bounded row pages across LSM reopen" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    const decoded_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/decoded", .{tmp.sub_path});
    const file_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source.afb", .{tmp.sub_path});
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 600, .shard_id = 601, .range_id = 601 };
    var source = try db_mod.DB.open(alloc, source_path, .{ .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
    defer source.close();
    var rows: [300]@import("types.zig").BatchWrite = undefined;
    for (&rows, 0..) |*row, i| row.* = .{ .key = try std.fmt.allocPrint(a, "row-{d:0>4}", .{i}), .value = "{\"id\":1}" };
    try source.batch(.{ .writes = &rows, .timestamp_ns = 900 });
    const keys = @import("../internal_keys.zig");
    const codec = @import("enrichment/artifact_codec.zig");
    const dense = try codec.encodeDenseEmbeddingAlloc(a, 123, &.{ 1, 0, 0 });
    var artifacts: std.ArrayList(@import("../docstore.zig").KVPair) = .empty;
    for (rows) |row| try artifacts.append(a, .{ .key = try keys.embeddingArtifactKeyForDocumentAlloc(a, row.key, "dense"), .value = dense });
    const sparse_key = try keys.embeddingArtifactKeyForDocumentAlloc(a, rows[0].key, "sparse");
    const chunk_key = try keys.chunkArtifactKeyAlloc(a, rows[0].key, "chunks", 1);
    const asset_key = try keys.documentUnitArtifactKeyAlloc(a, rows[0].key, "assets", "page");
    const edge_key = try keys.graphEdgeArtifactKeyAlloc(a, rows[0].key, "links", "related", rows[1].key);
    try artifacts.appendSlice(a, &.{
        .{ .key = sparse_key, .value = try codec.encodeSparseEmbeddingAlloc(a, 456, &.{ 3, 7 }, &.{ 0.5, 1 }) },
        .{ .key = chunk_key, .value = "{\"body\":\"chunk\"}" },
        .{ .key = asset_key, .value = "{\"text\":\"page\"}" },
        .{ .key = edge_key, .value = try codec.encodeGraphEdgeAlloc(a, null, 42, 0.5, 11, 22, "{}") },
    });
    try source.core.store.putBatch(artifacts.items, &.{});
    const identity = try source.relationalTopologyIdentity();
    const fence: @import("relational_integrity_topology.zig").Fence = .{ .role = .backup_snapshot, .transition_id = 55, .attempt = 1, .owner_group_id = 601, .peer_group_id = 601, .namespace = namespace, .catalog_digest = identity.catalog_digest, .admission_epoch = identity.next_epoch };
    try source.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null);
    const handle = try source.sealBackupCohort("portable-page", fence, .none);
    try source.applyRelationalTopologyControl(.{ .fence = fence, .action = .release }, null);
    const portable = @import("../portable_backup.zig");
    const proof: portable.CohortProof = .{ .seal = handle, .namespace = namespace };
    var file = try std.Io.Dir.cwd().createFile(std.testing.io, file_path, .{ .read = true });
    defer file.close(std.testing.io);
    var buffer: [65536]u8 = undefined;
    var writer = file.writer(std.testing.io, &buffer);
    try source.exportBackupCohortPortable(handle, &writer.interface, .{}, .none);
    try writer.end();
    try file.sync(std.testing.io);
    const size = (try file.stat(std.testing.io)).size;
    var calls: usize = 0;
    while (calls < 200) : (calls += 1) {
        var options = @import("config.zig").portable_decoder_lsm_options_default;
        options.read_runtime = @import("../lsm_backend/storage_io.zig").ReadRuntime.init(std.testing.io);
        var backend = try @import("../lsm_backend.zig").Backend.open(alloc, decoded_path, options);
        // Exactly the private decoder policy: each page's explicit sync, not
        // commit-time sync or graceful-close flushing, protects its checkpoint.
        defer backend.abandonAfterCrash();
        var store = try @import("../docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{ .name = "docs" }));
        defer store.close();
        const flushes_before = backend.write_stats.flushes;
        const done = try portable.importCohortFilePage(alloc, &store, std.testing.io, file, size, proof, @splat(7), 17, .none);
        try std.testing.expectEqual(@as(u64, 0), backend.write_stats.wal_sync_records);
        try std.testing.expectEqual(flushes_before, backend.write_stats.flushes);
        try std.testing.expectError(error.RestoreStagingScopeChanged, portable.importCohortFilePage(alloc, &store, std.testing.io, file, size, proof, @splat(8), 17, .none));
        if (done) {
            try std.testing.expect(try portable.importCohortFilePage(alloc, &store, std.testing.io, file, size, proof, @splat(7), 17, .none));
            try @import("doc_identity.zig").validatePrimaryDocumentCoverageAlloc(alloc, &store);
            const stats = try @import("doc_identity.zig").fullStatsFromStore(&store);
            try std.testing.expectEqual(@as(u64, 300), stats.live_ordinals);
            for (rows) |row| {
                const key = try @import("../internal_keys.zig").documentKeyAlloc(a, row.key);
                const value = try store.get(a, key);
                try std.testing.expectEqualStrings(row.value, value);
                const artifact_value = try store.get(a, try keys.embeddingArtifactKeyForDocumentAlloc(a, row.key, "dense"));
                try std.testing.expect(!(try codec.decodeHeader(artifact_value)).flags.has_source_hash);
                try std.testing.expectEqualSlices(f32, &.{ 1, 0, 0 }, try codec.decodeDenseEmbeddingAlloc(a, artifact_value));
            }
            const sparse_value = try store.get(a, sparse_key);
            try std.testing.expect(!(try codec.decodeHeader(sparse_value)).flags.has_source_hash);
            const sparse = try codec.decodeSparseEmbeddingAlloc(a, sparse_value);
            try std.testing.expectEqualSlices(u32, &.{ 3, 7 }, sparse.indices);
            try std.testing.expectEqualSlices(f32, &.{ 0.5, 1 }, sparse.values);
            try std.testing.expectEqualStrings("{\"body\":\"chunk\"}", try store.get(a, chunk_key));
            try std.testing.expectEqualStrings("{\"text\":\"page\"}", try store.get(a, asset_key));
            const edge_value = try store.get(a, edge_key);
            try std.testing.expect(codec.isPortableUnboundGraphEdge(edge_value));
            try std.testing.expectEqual(@as(f64, 0.5), (try codec.decodeGraphEdgeAlloc(a, edge_value)).weight);
            break;
        }
    } else return error.RestoreWorkerDidNotConverge;
    try std.testing.expect(calls > 300 / 17);
}

fn binding(db: *db_mod.DB, kind: catalog.Kind, name: []const u8) !integrity.Generation {
    const alloc = std.testing.allocator;
    const raw = try db.core.getStoreValue(alloc, catalog.key) orelse return error.MissingIntegrityCatalog;
    defer alloc.free(raw);
    var loaded = try catalog.decode(alloc, raw);
    defer loaded.deinit();
    return (loaded.find(kind, name) orelse return error.MissingIntegrityBinding).generation;
}

fn generationSet(db: *db_mod.DB) ![32]u8 {
    const alloc = std.testing.allocator;
    const raw = try db.core.getStoreValue(alloc, catalog.key) orelse return error.MissingIntegrityCatalog;
    defer alloc.free(raw);
    var loaded = try catalog.decode(alloc, raw);
    defer loaded.deinit();
    return @import("relational_integrity_activation.zig").generationSet(loaded);
}

test "relational integrity scoped two phase resolution mirrors binary claims through HA" {
    const alloc = std.testing.allocator;
    const restore = @import("restore_staging.zig");
    const primary_mod = @import("../hot_standby/primary.zig");
    const effects = @import("../hot_standby/effects.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const source_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/ha-2pc-source", .{tmp.sub_path});
    const target_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/ha-2pc-target", .{tmp.sub_path});
    const replica_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/ha-2pc-replica", .{tmp.sub_path});
    const source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var source = try db_mod.DB.open(alloc, source_path, source_options);
        defer source.close();
        try source.setSchemaJson(alloc, schema);
    }
    var read_options = source_options;
    read_options.open_mode = .query_readonly;
    var source = try db_mod.DB.open(alloc, source_path, read_options);
    defer source.close();
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var target = try db_mod.DB.open(alloc, target_path, options);
    defer target.close();
    var replica = try db_mod.DB.open(alloc, replica_path, options);
    defer replica.close();
    try target.setSchemaJson(alloc, schema);
    try replica.setSchemaJson(alloc, schema);
    const encoded = try @import("../schema.zig").serializeSchema(owned, target.core.schema.?);
    const scope: restore.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = options.identity_namespace.?, .target_schema_digest = restore.digest(encoded) };
    for ([_]*db_mod.DB{ &target, &replica }) |db| {
        try db.reserveRestoreStagingScoped(alloc, scope);
        try db.beginRestoreStaging(alloc, scope);
        var page = try db.prepareRestoreStagingPage(alloc, scope, &source, 1, .none);
        defer page.deinit();
        try db.batch(page.batch orelse return error.TestUnexpectedResult);
        var status = (try db.restoreStagingStatus(alloc)).?;
        defer status.deinit();
        try std.testing.expectEqual(.imported, status.value.phase);
    }
    const log_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/ha-2pc-log", .{tmp.sub_path}, 0);
    const slots_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/ha-2pc-slots", .{tmp.sub_path}, 0);
    var primary = try primary_mod.Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 10, .shard_id = 11 }, .{});
    defer primary.close();
    target.ha_async_batch_mirror = .{ .primary = &primary, .sync_policy = .{ .mode = .async } };
    defer target.ha_async_batch_mirror = null;
    var view = target.core.acquireSchemaView().?;
    defer view.release();
    var tuple_plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
    defer tuple_plan.deinit();
    var tuple: std.ArrayList(u8) = .empty;
    defer tuple.deinit(alloc);
    _ = try tuple_plan.appendValues(alloc, &tuple, &.{.{ .integer = 9001 }});
    const address = try integrity.Address.init(try binding(&target, .unique, "pk"), tuple.items);
    const transaction = try target.beginTransactionScoped(@splat(18), 100, 100, &.{}, false, false, scope.digest());
    try target.writeTransaction(transaction, .{ .restore_staging_scope = scope.digest(), .relational_schema_version = 1, .integrity_commands = &.{.{ .address = address, .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = "p", .schema_version = 1 } } }} });
    try target.resolveReplicatedTransactionAtRaftEntry(transaction, .committed, 200, .full_index, .none, .{ .term = 1, .index = 1 }, null);
    try target.resolveReplicatedTransactionAtRaftEntry(transaction, .committed, 200, .full_index, .none, .{ .term = 1, .index = 1 }, null);
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    var entry = (try primary.log.entryAt(alloc, 1)).?;
    defer entry.deinit(alloc);
    var decoded = try effects.decodeBatchMutationRequest(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &scope.digest(), &decoded.value.request.restore_staging_scope.?);
    try std.testing.expect(decoded.value.request.restore_staging == null);
    try replica.applyHAReplicationRecord(entry.record);
    const claim_key = address.claimKey();
    const expected = (try target.core.getStoreValue(alloc, &claim_key)).?;
    defer alloc.free(expected);
    const actual = (try replica.core.getStoreValue(alloc, &claim_key)).?;
    defer alloc.free(actual);
    try std.testing.expectEqualSlices(u8, expected, actual);
}

test "relational integrity live two phase HA replay preserves rows and binary claim reference effects" {
    const alloc = std.testing.allocator;
    const primary_mod = @import("../hot_standby/primary.zig");
    const effects = @import("../hot_standby/effects.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const primary_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/live-primary", .{tmp.sub_path});
    const replica_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/live-replica", .{tmp.sub_path});
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try db_mod.DB.open(alloc, primary_path, options);
    defer db.close();
    var replica = try db_mod.DB.open(alloc, replica_path, options);
    defer replica.close();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, schema);
    try replica.setSchemaJson(alloc, schema);
    const log_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/live-log", .{tmp.sub_path}, 0);
    const slots_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/live-slots", .{tmp.sub_path}, 0);
    var primary = try primary_mod.Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 10, .shard_id = 11 }, .{});
    defer primary.close();
    db.ha_async_batch_mirror = .{ .primary = &primary, .sync_policy = .{ .mode = .async } };
    defer db.ha_async_batch_mirror = null;
    var view = db.core.acquireSchemaView().?;
    defer view.release();
    var plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
    defer plan.deinit();
    var tuple: std.ArrayList(u8) = .empty;
    defer tuple.deinit(alloc);
    _ = try plan.appendValues(alloc, &tuple, &.{.{ .integer = 9001 }});
    const address = try integrity.Address.init(try binding(&db, .unique, "pk"), tuple.items);
    const claim: integrity.Claim = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = "p", .schema_version = 1 };
    const reference: integrity.Reference = .{ .child_table = "children", .child_key = "c", .constraint_name = "fk", .constraint_generation = @splat(250) };
    const txn = try db.beginTransactionWithId(@splat(31), 100);
    try db.writeTransaction(txn, .{ .relational_schema_version = 1, .relational_integrity_generation_set = try generationSet(&db), .writes = &.{.{ .key = "p", .value = "{\"id\":9001}" }}, .integrity_commands = &.{ .{ .address = address, .operation = .{ .establish = claim } }, .{ .address = address, .operation = .{ .attach = reference } } } });
    try db.commitTransaction(txn, 200);
    try std.testing.expect(primary.lastLsn() > 0);
    // A replicated quiescence fence must still admit authoritative effects
    // from participants prepared before that fence, just like local resolve.
    const owner = try replica.relationalTopologyIdentity();
    const fence: @import("relational_integrity_topology.zig").Fence = .{
        .transition_id = 701,
        .attempt = 1,
        .owner_group_id = 11,
        .peer_group_id = 11,
        .role = .backup_snapshot,
        .namespace = owner.namespace,
        .catalog_digest = owner.catalog_digest,
    };
    try replica.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null);
    var lsn: u64 = 1;
    while (lsn <= primary.lastLsn()) : (lsn += 1) {
        var entry = (try primary.log.entryAt(alloc, lsn)).?;
        defer entry.deinit(alloc);
        var decoded = try effects.decodeBatchMutationRequest(alloc, entry.record);
        defer decoded.deinit();
        try std.testing.expect(decoded.value.request.restore_staging_scope == null);
        // The same bytes remain forbidden to the unauthenticated raw batch API.
        try std.testing.expectError(error.InvalidIntegrityOperation, replica.batch(decoded.value.request));
        try replica.applyHAReplicationRecord(entry.record);
        try replica.applyHAReplicationRecord(entry.record);
    }
    try replica.applyRelationalTopologyControl(.{ .fence = fence, .action = .release }, null);
    var row = (try replica.lookup(alloc, "p", .{})).?;
    defer row.deinit(alloc);
    const claim_key = address.claimKey();
    const reference_key = try reference.key(address);
    for ([_][]const u8{ &claim_key, &reference_key }) |key| {
        const expected = (try db.core.getStoreValue(alloc, key)).?;
        defer alloc.free(expected);
        const actual = (try replica.core.getStoreValue(alloc, key)).?;
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, expected, actual);
    }
    const remove = try db.beginTransactionWithId(@splat(32), 300);
    try db.writeTransaction(remove, .{ .relational_schema_version = 1, .relational_integrity_generation_set = try generationSet(&db), .deletes = &.{"p"}, .integrity_commands = &.{ .{ .address = address, .operation = .{ .detach = reference } }, .{ .address = address, .operation = .{ .release = .{ .parent_table = "parents", .parent_key = "p" } } } } });
    try db.commitTransaction(remove, 400);
    while (lsn <= primary.lastLsn()) : (lsn += 1) {
        var entry = (try primary.log.entryAt(alloc, lsn)).?;
        defer entry.deinit(alloc);
        try replica.applyHAReplicationRecord(entry.record);
    }
    try std.testing.expect((try replica.lookup(alloc, "p", .{})) == null);
    for ([_][]const u8{ &claim_key, &reference_key }) |key| try std.testing.expect((try replica.core.getStoreValue(alloc, key)) == null);
    const before_invalid = try replica.haAppliedReplicationLsn();
    const invalid_address = try integrity.Address.init(@splat(199), tuple.items);
    const invalid_key = invalid_address.claimKey();
    const invalid_value = try claim.encode(alloc, invalid_address);
    defer alloc.free(invalid_value);
    const invalid_lsn = try effects.appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = &invalid_key, .value = invalid_value }},
    }, .{});
    var invalid_entry = (try primary.log.entryAt(alloc, invalid_lsn)).?;
    defer invalid_entry.deinit(alloc);
    try std.testing.expectError(error.IntegrityCatalogChanged, replica.applyHAReplicationRecord(invalid_entry.record));
    try std.testing.expectEqual(before_invalid, try replica.haAppliedReplicationLsn());
}

test "relational integrity DB transactions atomically preserve cross-table parent dependencies" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var parent_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const parent_path = try std.fmt.bufPrint(&parent_path_buf, ".zig-cache/tmp/{s}/parent", .{tmp.sub_path});
    var child_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const child_path = try std.fmt.bufPrint(&child_path_buf, ".zig-cache/tmp/{s}/child", .{tmp.sub_path});
    var parent = try db_mod.DB.open(alloc, parent_path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 100, .shard_id = 101 }, .primary_backend = .{ .lsm = .{} } });
    defer parent.close();
    var child = try db_mod.DB.open(alloc, child_path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 200, .shard_id = 201 }, .primary_backend = .{ .lsm = .{} } });
    defer child.close();
    try parent.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"parent_pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try child.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"parent_fk","child_columns":["parent_id"],"parent_table":"parents","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    var portable: std.ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try std.testing.expectError(error.CoordinatedConstraintPortableBackupUnsupported, @import("../portable_backup.zig").exportPortable(alloc, parent.core.store, &portable));
    try std.testing.expectEqual(@as(usize, 0), portable.items.len);
    var view = parent.core.acquireSchemaView().?;
    defer view.release();
    var plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
    defer plan.deinit();
    var tuple: std.ArrayList(u8) = .empty;
    defer tuple.deinit(alloc);
    _ = try plan.appendValues(alloc, &tuple, &.{.{ .integer = 1 }});
    const address = try integrity.Address.init(try binding(&parent, .unique, "parent_pk"), tuple.items);
    const claim: integrity.Claim = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = "p", .schema_version = 1 };
    const reference: integrity.Reference = .{ .child_table = "children", .child_key = "c", .constraint_name = "parent_fk", .constraint_generation = try binding(&child, .foreign_key, "parent_fk") };
    const create = try parent.beginTransactionWithId(@splat(11), 100);
    // A client schema epoch is not evidence that the internal FK planner ran.
    try std.testing.expectError(error.ForeignKeyCoordinationRequired, parent.writeTransaction(create, .{
        .relational_schema_version = 1,
        .writes = &.{.{ .key = "p", .value = "{\"id\":1}" }},
    }));
    try std.testing.expectError(error.IntegrityCatalogChanged, parent.writeTransaction(create, .{
        .relational_schema_version = 1,
        .relational_integrity_generation_set = @splat(0),
        .writes = &.{.{ .key = "p", .value = "{\"id\":1}" }},
    }));
    try parent.writeTransaction(create, .{
        .relational_schema_version = 1,
        .relational_integrity_generation_set = try generationSet(&parent),
        .writes = &.{.{ .key = "p", .value = "{\"id\":1}" }},
        .integrity_commands = &.{.{ .address = address, .operation = .{ .establish = claim } }},
    });
    // No primary row or claim is visible before terminal resolution.
    try std.testing.expect((try parent.lookup(alloc, "p", .{})) == null);
    try parent.commitTransaction(create, 200);

    const attach = try parent.beginTransactionWithId(@splat(12), 300);
    _ = try child.beginTransactionWithId(attach, 300);
    const parent_prepare: @import("types.zig").TransactionIntentRequest = .{
        .relational_schema_version = 1,
        .integrity_commands = &.{.{ .address = address, .operation = .{ .attach = reference } }},
    };
    try parent.writeTransaction(attach, parent_prepare);
    try parent.writeTransaction(attach, parent_prepare); // cumulative prepare retry is idempotent
    try child.writeTransaction(attach, .{ .relational_schema_version = 1, .relational_integrity_generation_set = try generationSet(&child), .writes = &.{.{ .key = "c", .value = "{\"id\":2,\"parent_id\":1}" }} });
    const remove = try parent.beginTransactionWithId(@splat(13), 400);
    const release: @import("types.zig").TransactionIntentRequest = .{
        .relational_schema_version = 1,
        .relational_integrity_generation_set = try generationSet(&parent),
        .deletes = &.{"p"},
        .integrity_commands = &.{.{ .address = address, .operation = .{ .release = .{ .parent_table = "parents", .parent_key = "p" } } }},
    };
    try std.testing.expectError(error.IntentConflict, parent.writeTransaction(remove, release));
    try parent.commitTransaction(attach, 500);
    try child.commitTransaction(attach, 500);
    try std.testing.expectError(error.ForeignKeyReferenced, parent.writeTransaction(remove, release));
    try parent.abortTransaction(remove, 600);

    const detach = try parent.beginTransactionWithId(@splat(14), 700);
    _ = try child.beginTransactionWithId(detach, 700);
    try parent.writeTransaction(detach, .{ .relational_schema_version = 1, .integrity_commands = &.{.{ .address = address, .operation = .{ .detach = reference } }} });
    try child.writeTransaction(detach, .{ .relational_schema_version = 1, .relational_integrity_generation_set = try generationSet(&child), .deletes = &.{"c"} });
    try child.commitTransaction(detach, 800);
    try parent.commitTransaction(detach, 800);
    const finish = try parent.beginTransactionWithId(@splat(15), 900);
    try parent.writeTransaction(finish, release);
    try parent.commitTransaction(finish, 1000);
    try std.testing.expect((try parent.lookup(alloc, "p", .{})) == null);
    try std.testing.expect((try child.lookup(alloc, "c", .{})) == null);
    var read = try parent.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(&address.claimKey()));
    try std.testing.expectError(error.NotFound, read.get(&(try reference.key(address))));
}

fn activateOnePage(db: *db_mod.DB, txn_byte: u8) !bool {
    const activation = @import("relational_integrity_activation.zig");
    const types = @import("types.zig");
    const alloc = std.testing.allocator;
    var page = (try activation.Page.prepare(alloc, null, db.core, .{ .rows = 1, .records = 64 })) orelse return true;
    defer page.deinit();
    const id = try binding(db, .unique, "id_unique");
    var view = db.core.acquireSchemaView().?;
    defer view.release();
    var plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
    defer plan.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const commands = try owned.alloc(integrity.Command, page.rows.rows.len);
    const predicates = try owned.alloc(types.TransactionVersionPredicate, page.rows.rows.len);
    for (page.rows.rows, commands, predicates) |row, *command, *predicate| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row.json, .{});
        defer parsed.deinit();
        var tuple: std.ArrayList(u8) = .empty;
        _ = try plan.appendValues(owned, &tuple, &.{.{ .integer = parsed.value.object.get("id").?.integer }});
        command.* = .{ .address = try integrity.Address.init(id, tuple.items), .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = row.key, .schema_version = view.version() } } };
        predicate.* = .{ .key = row.key, .expected_version = row.version };
    }
    const transaction = try db.beginTransactionWithId(@splat(txn_byte), 500);
    try db.writeTransaction(transaction, .{ .relational_schema_version = view.version(), .integrity_commands = commands, .predicates = predicates, .relational_activation = page.command });
    try db.commitTransaction(transaction, 600);
    return page.progress.state == .enforced;
}

test "relational integrity topology quiesces new work while old decisions drain and resumes after release" {
    const alloc = std.testing.allocator;
    const topology = @import("relational_integrity_topology.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fenced", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{
        .identity_namespace = .{ .table_id = 500, .shard_id = 501, .range_id = 502 },
        .start_optional_runtimes = false,
        .start_index_workers = false,
        .primary_backend = .{ .lsm = .{} },
    });
    defer db.close();
    const old = try db.beginTransactionWithId(@splat(71), 100);
    try db.writeTransaction(old, .{ .writes = &.{.{ .key = "a", .value = "{\"v\":1}" }} });
    const owner = try db.relationalTopologyIdentity();
    const fence: topology.Fence = .{
        .transition_id = 700,
        .attempt = 1,
        .owner_group_id = 501,
        .peer_group_id = 501,
        .role = .backup_snapshot,
        .namespace = owner.namespace,
        .catalog_digest = owner.catalog_digest,
    };
    try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null);
    try std.testing.expect(!(try db.relationalTopologyStatus()).drained);
    try std.testing.expectError(error.IntegrityTopologyBusy, db.batch(.{ .writes = &.{.{ .key = "b", .value = "{}" }} }));
    const fresh = try db.beginTransactionWithId(@splat(72), 200);
    try std.testing.expectError(error.IntegrityTopologyBusy, db.writeTransaction(fresh, .{ .writes = &.{.{ .key = "b", .value = "{}" }} }));
    try db.abortTransaction(fresh, 210);
    try db.writeTransaction(old, .{ .writes = &.{.{ .key = "a", .value = "{\"v\":1}" }} });
    try db.commitTransaction(old, 220);
    try std.testing.expect((try db.relationalTopologyStatus()).drained);
    try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .release }, null);
    try db.batch(.{ .writes = &.{.{ .key = "b", .value = "{}" }} });
    try std.testing.expectError(error.IntegrityTopologyCompleted, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null));
    var cancelled = fence;
    cancelled.transition_id += 1;
    cancelled.admission_epoch = (try db.relationalTopologyIdentity()).next_epoch;
    try db.batchReplicatedApply(.{ .relational_topology = .{ .fence = cancelled, .action = .cancel } });
    try std.testing.expectError(error.IntegrityTopologyCompleted, db.batchReplicatedApply(.{ .relational_topology = .{ .fence = cancelled, .action = .begin } }));
    var newer = cancelled;
    newer.transition_id += 1;
    newer.admission_epoch = (try db.relationalTopologyIdentity()).next_epoch;
    try db.batch(.{ .relational_topology = .{ .fence = newer, .action = .begin } });
    try db.batch(.{ .relational_topology = .{ .fence = newer, .action = .release } });
    try std.testing.expectError(error.IntegrityTopologyCompleted, db.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null));
    try std.testing.expectError(error.IntegrityTopologyCompleted, db.applyRelationalTopologyControl(.{ .fence = cancelled, .action = .begin }, null));
    var aborted = newer;
    aborted.transition_id += 10;
    aborted.role = .split_source;
    aborted.admission_epoch = (try db.relationalTopologyIdentity()).next_epoch;
    try db.applyRelationalTopologyControl(.{ .fence = aborted, .action = .abort_transition }, null);
    // A rollback need not know the epoch assigned to an ambiguously delivered
    // begin. The transition tombstone rejects it even with a newer epoch.
    aborted.admission_epoch += 100;
    try std.testing.expectError(error.IntegrityTopologyCompleted, db.applyRelationalTopologyControl(.{ .fence = aborted, .action = .begin }, null));
}

test "relational integrity topology handoff transfers routed companions with restartable page CAS" {
    const alloc = std.testing.allocator;
    const topology = @import("relational_integrity_topology.zig");
    const handoff = @import("relational_integrity_handoff.zig");
    const retirement = @import("relational_integrity_generation_retirement.zig");
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    const destination_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/destination", .{tmp.sub_path});
    var source = try db_mod.DB.open(alloc, source_path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 600, .shard_id = 601 }, .primary_backend = .{ .lsm = .{} } });
    defer source.close();
    var destination = try db_mod.DB.open(alloc, destination_path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 600, .shard_id = 602 }, .primary_backend = .{ .lsm = .{} } });
    defer destination.close();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try source.setSchemaJson(alloc, schema);
    try destination.setSchemaJson(alloc, schema);
    const generation = try binding(&source, .unique, "pk");
    {
        var view = source.core.acquireSchemaView().?;
        defer view.release();
        var plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
        defer plan.deinit();
        var tuple: std.ArrayList(u8) = .empty;
        defer tuple.deinit(alloc);
        _ = try plan.appendValues(alloc, &tuple, &.{.{ .integer = 999 }});
        const address = try integrity.Address.init(generation, tuple.items);
        const create = try source.beginTransactionWithId(@splat(75), 100);
        try source.writeTransaction(create, .{ .relational_schema_version = 1, .relational_integrity_generation_set = try generationSet(&source), .writes = &.{.{ .key = "z", .value = "{\"id\":999}" }}, .integrity_commands = &.{.{ .address = address, .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = "z", .schema_version = 1 } } }} });
        try source.commitTransaction(create, 200);
    }
    // Populate more than one bounded page. The second physical namespace
    // contains child references whose matching claims were sent first.
    var kept_address: ?integrity.Address = null;
    for (0..300) |i| {
        const tuple = try std.fmt.allocPrint(owned, "tuple-{d}", .{i});
        const address = try integrity.Address.init(generation, tuple);
        if (std.mem.order(u8, &address.routing, "m") != .lt) kept_address = address;
        const claim: integrity.Claim = .{ .tuple = tuple, .parent_table = "parents", .parent_key = tuple, .schema_version = 1 };
        const reference: integrity.Reference = .{ .child_table = "children", .child_key = tuple, .constraint_name = "fk", .constraint_generation = @splat(9) };
        try source.core.store.put(&address.claimKey(), try claim.encode(owned, address));
        try source.core.store.put(&(try reference.key(address)), try reference.encode(owned, address));
    }
    const source_owner = try source.relationalTopologyIdentity();
    const destination_owner = try destination.relationalTopologyIdentity();
    const retired: integrity.Reference = .{ .child_table = "children", .child_key = "old", .constraint_name = "fk", .constraint_generation = @splat(9) };
    const parent_fence: topology.Fence = .{ .transition_id = 799, .attempt = 1, .owner_group_id = 601, .peer_group_id = 700, .role = .truncate_parent, .namespace = source_owner.namespace, .catalog_digest = source_owner.catalog_digest, .admission_epoch = source_owner.next_epoch };
    try source.applyRelationalTopologyControl(.{ .action = .begin, .fence = parent_fence }, null);
    try source.applyRelationalTopologyControl(.{ .action = .stage_parent_retirement, .fence = parent_fence, .parent_retirement = .{ .plan_digest = @splat(5), .entries = &.{.{ .child_table_id = 700, .child_table_name = "children", .constraint_name = "fk", .generation = retired.constraint_generation, .next_generation = @splat(6) }} } }, null);
    try source.applyRelationalTopologyControl(.{ .action = .activate_parent_retirement, .fence = parent_fence, .parent_activation = .{ .plan_id = @splat(4), .plan_digest = @splat(5), .publication_digest = retirement.publicationDigest(@splat(4), @splat(5)) } }, null);
    const parent_acknowledge: topology.Command = .{ .action = .acknowledge_parent_retirement, .fence = parent_fence, .parent_activation = .{ .plan_id = @splat(4), .plan_digest = @splat(5), .publication_digest = retirement.publicationDigest(@splat(4), @splat(5)) } };
    const source_fence: topology.Fence = .{ .transition_id = 800, .attempt = 1, .owner_group_id = 601, .peer_group_id = 602, .role = .split_source, .namespace = source_owner.namespace, .catalog_digest = source_owner.catalog_digest, .admission_epoch = source_owner.next_epoch + 1 };
    const destination_fence: topology.Fence = .{ .transition_id = 800, .attempt = 1, .owner_group_id = 602, .peer_group_id = 601, .role = .split_destination, .namespace = destination_owner.namespace, .catalog_digest = destination_owner.catalog_digest };
    try std.testing.expectError(error.GenerationRetirementAcknowledgementPending, source.applyRelationalTopologyControl(.{ .action = .begin, .fence = source_fence }, null));
    try source.applyRelationalTopologyControl(parent_acknowledge, null);
    try source.applyRelationalTopologyControl(.{ .action = .begin, .fence = source_fence }, null);
    try destination.applyRelationalTopologyControl(.{ .action = .begin, .fence = destination_fence }, null);
    const manifest = try source.relationalHandoffManifest(owned, source_fence, destination_fence, "m", "", 10);
    try destination.applyRelationalTopologyControl(.{ .action = .transfer, .fence = destination_fence, .transfer = .{ .begin = manifest } }, null);
    var page_count: usize = 0;
    while (true) {
        var read = try destination.core.store.beginProbeTxn();
        const progress = try handoff.loadProgress(owned, &read);
        read.abort();
        if (progress.value.exhausted) break;
        const page = try source.relationalHandoffPage(owned, manifest, progress.value);
        try destination.applyRelationalTopologyControl(.{ .action = .transfer, .fence = destination_fence, .transfer = .{ .page = page } }, null);
        try destination.applyRelationalTopologyControl(.{ .action = .transfer, .fence = destination_fence, .transfer = .{ .page = page } }, null);
        page_count += 1;
    }
    try std.testing.expect(page_count >= 2);
    while (true) {
        var read = try destination.core.store.beginProbeTxn();
        const progress = try handoff.loadProgress(owned, &read);
        read.abort();
        if (progress.value.ready) break;
        try destination.applyRelationalTopologyControl(.{ .action = .transfer, .fence = destination_fence, .transfer = .{ .finish = .{ .sequence = progress.value.sequence, .digest = progress.value.digest } } }, null);
    }
    var read = try destination.core.store.beginProbeTxn();
    defer read.abort();
    try std.testing.expect(try retirement.isRetired(&read, retired));
    const accepted = (try @import("relational_integrity_generation_admission.zig").load(&read, retired.child_table, retired.constraint_name)).?;
    try std.testing.expectEqual(@as(integrity.Generation, @splat(6)), accepted.active_generation.?);
    const transferred_gc = try retirement.GcProgress.decode(try read.get(retirement.gc_progress_key));
    try std.testing.expect(!transferred_gc.complete);
    try std.testing.expectEqualStrings("", transferred_gc.cursor);
    const address = kept_address orelse return error.TestUnexpectedResult;
    try integrity.validateTransferredCompanions(&read, &address.claimKey(), try read.get(&address.claimKey()));
    // Metadata completion alone never releases the destination write fence.
    try std.testing.expect((try destination.relationalTopologyStatus()).fence != null);
    const replication: @import("types.zig").SplitReplicationContext = .{ .transition_id = 800, .attempt_epoch = 1, .source_group_id = 601, .destination_group_id = 602, .identity_namespace = destination_owner.namespace, .bootstrap_sequence = 10, .operation = .checkpoint, .sequence = 10 };
    const checkpoint: @import("types.zig").SplitReplicationCheckpoint = .{ .kind = .destination_begin, .transition_id = 800, .attempt_epoch = 1, .source_group_id = 601, .destination_group_id = 602, .range_start = "m", .range_end = "", .delta_sequence = 10 };
    try destination.batchRaftReplicatedApply(.{ .split_replication = replication, .split_checkpoint = checkpoint }, .{ .term = 1, .index = 10 });
    var row_replication = replication;
    row_replication.operation = .bootstrap_chunk;
    try destination.batchRaftReplicatedApply(.{ .split_replication = row_replication, .writes = &.{.{ .key = "z", .value = "{\"id\":999}" }} }, .{ .term = 1, .index = 11 });
    var complete = checkpoint;
    complete.kind = .destination_complete;
    try destination.batchRaftReplicatedApply(.{ .split_replication = replication, .split_checkpoint = complete }, .{ .term = 1, .index = 12 });
    try source.batchRaftReplicatedApply(.{ .split_transition = .{ .kind = .finalize, .transition_id = 800, .attempt_epoch = 1, .destination_group_id = 602, .split_key = "m" } }, .{ .term = 1, .index = 12 });
    try std.testing.expect((try source.relationalTopologyStatus()).fence == null);
    try destination.applyRelationalTopologyControl(.{ .action = .release, .fence = destination_fence }, null);
    try std.testing.expect((try destination.relationalTopologyStatus()).fence == null);
    try std.testing.expectEqualStrings("m", source.getRange().end);
    try std.testing.expectEqualStrings("m", destination.getRange().start);
    const row = (try destination.get(alloc, "z")) orelse return error.TestUnexpectedResult;
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"id\":999}", row);
    while (true) {
        try source.applyRelationalTopologyControl(.{ .action = .prune, .fence = source_fence }, null);
        var source_read = try source.core.store.beginProbeTxn();
        defer source_read.abort();
        var progress = try std.json.parseFromSlice(handoff.PruneProgress, owned, try source_read.get(handoff.prune_key), .{});
        defer progress.deinit();
        if (progress.value.complete) {
            try std.testing.expectError(error.NotFound, source_read.get(&address.claimKey()));
            break;
        }
    }
    var retry_destination = destination_fence;
    retry_destination.transition_id += 1;
    retry_destination.admission_epoch = (try destination.relationalTopologyIdentity()).next_epoch;
    try std.testing.expectError(error.IntegrityHandoffDestinationResetRequired, destination.applyRelationalTopologyControl(.{ .action = .begin, .fence = retry_destination }, null));
}

test "relational integrity resolves private keys on non-first logical owner" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
    var db = try db_mod.DB.open(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 970, .shard_id = 971 }, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    try db.updateRange(.{ .start = "m", .end = "" });
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    const generation = try binding(&db, .unique, "pk");
    var view = db.core.acquireSchemaView().?;
    defer view.release();
    var plan = try tuples.TuplePlan.init(owned, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
    defer plan.deinit();
    var tuple: std.ArrayList(u8) = .empty;
    var address: integrity.Address = undefined;
    var value: i64 = 0;
    while (value < 1000) : (value += 1) {
        tuple.clearRetainingCapacity();
        _ = try plan.appendValues(owned, &tuple, &.{.{ .integer = value }});
        address = try integrity.Address.init(generation, tuple.items);
        if (db.getRange().contains(&address.routing)) break;
    } else return error.TestUnexpectedResult;
    const json = try std.fmt.allocPrint(owned, "{{\"id\":{d}}}", .{value});
    const txn = try db.beginTransactionWithId(@splat(121), 100);
    try db.writeTransaction(txn, .{ .relational_schema_version = 1, .relational_integrity_generation_set = try generationSet(&db), .writes = &.{.{ .key = "z", .value = json }}, .integrity_commands = &.{.{ .address = address, .operation = .{ .establish = .{ .tuple = tuple.items, .parent_table = "parents", .parent_key = "z", .schema_version = 1 } } }} });
    try db.commitTransaction(txn, 200);
    const row = (try db.get(owned, "z")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(json, row);
    var read = try db.core.store.beginProbeTxn();
    defer read.abort();
    _ = try read.get(&address.claimKey());
}

test "relational integrity topology merge rollback prunes imported claims before unfreezing" {
    try testMergeIntegrityHandoff(true, false, false);
}

test "relational integrity topology merge finalize keeps both ownership slices after reopen" {
    try testMergeIntegrityHandoff(false, false, false);
}

test "relational integrity topology merge rollback before transfer releases both fences" {
    try testMergeIntegrityHandoff(true, true, false);
}

test "relational integrity topology merge HA replay preserves imported ownership through finalize" {
    try testMergeIntegrityHandoff(false, false, true);
}

test "relational integrity topology merge HA replay preserves rollback prune and abort" {
    try testMergeIntegrityHandoff(true, false, true);
}

fn testMergeIntegrityHandoff(comptime rollback: bool, comptime empty: bool, comptime ha: bool) !void {
    const alloc = std.testing.allocator;
    const topology = @import("relational_integrity_topology.zig");
    const handoff = @import("relational_integrity_handoff.zig");
    const types = @import("types.zig");
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    const destination_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/destination", .{tmp.sub_path});
    var primary: @import("../hot_standby/primary.zig").Primary = if (ha) try @import("../hot_standby/primary.zig").Primary.open(alloc, try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/ha-log", .{tmp.sub_path}, 0), try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/ha-slots", .{tmp.sub_path}, 0), .{ .cluster_id = 901, .timeline_id = 1, .epoch = 1 }, .{}) else undefined;
    defer if (ha) primary.close();
    const source_options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 900, .shard_id = 901 }, .primary_backend = .{ .lsm = .{} } };
    const destination_options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 900, .shard_id = 902 }, .primary_backend = .{ .lsm = .{} }, .ha_async_batch_mirror = if (ha) .{ .primary = &primary } else null, .ha_write_gate = if (ha) .{ .primary = &primary } else null };
    var source = try db_mod.DB.open(alloc, source_path, source_options);
    defer source.close();
    var destination = try db_mod.DB.open(alloc, destination_path, destination_options);
    defer destination.close();
    try source.updateRange(.{ .start = "m", .end = "" });
    try destination.updateRange(.{ .start = "", .end = "m" });
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try source.setSchemaJson(alloc, schema);
    try destination.setSchemaJson(alloc, schema);
    var standby_options = destination_options;
    standby_options.ha_async_batch_mirror = null;
    standby_options.ha_write_gate = null;
    var standby: db_mod.DB = if (ha) try db_mod.DB.open(alloc, try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/standby", .{tmp.sub_path}), standby_options) else undefined;
    defer if (ha) standby.close();
    if (ha) {
        try standby.updateRange(.{ .start = "", .end = "m" });
        try standby.setSchemaJson(alloc, schema);
    }
    const generation = try binding(&source, .unique, "pk");
    var imported: ?integrity.Address = null;
    var retained: ?integrity.Address = null;
    for (0..800) |i| {
        const tuple = try std.fmt.allocPrint(owned, "merge-{d}", .{i});
        const address = try integrity.Address.init(generation, tuple);
        const claim: integrity.Claim = .{ .tuple = tuple, .parent_table = "parents", .parent_key = tuple, .schema_version = 1 };
        if (std.mem.order(u8, &address.routing, "m") != .lt) {
            try source.core.store.put(&address.claimKey(), try claim.encode(owned, address));
            imported = address;
        } else {
            try destination.core.store.put(&address.claimKey(), try claim.encode(owned, address));
            if (ha) try standby.core.store.put(&address.claimKey(), try claim.encode(owned, address));
            retained = address;
        }
    }
    const replay_start = if (ha) primary.nextLsn() else 0;
    const source_owner = try source.relationalTopologyIdentity();
    const destination_owner = try destination.relationalTopologyIdentity();
    const source_fence: topology.Fence = .{ .transition_id = 980, .attempt = 1, .owner_group_id = 901, .peer_group_id = 902, .role = .merge_source, .namespace = source_owner.namespace, .catalog_digest = source_owner.catalog_digest };
    const destination_fence: topology.Fence = .{ .transition_id = 980, .attempt = 1, .owner_group_id = 902, .peer_group_id = 901, .role = .merge_destination, .namespace = destination_owner.namespace, .catalog_digest = destination_owner.catalog_digest };
    try source.applyRelationalTopologyControl(.{ .action = .begin, .fence = source_fence }, null);
    try destination.batch(.{ .relational_topology = .{ .action = .begin, .fence = destination_fence } });
    var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 980, .donor_group_id = 901, .receiver_group_id = 902, .receiver_base_start = "", .receiver_base_end = "m", .merged_start = "", .merged_end = "" };
    try destination.batchRaftReplicatedApply(.{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 1 });
    if (empty) {
        checkpoint.kind = .rollback;
        try destination.batchRaftReplicatedApply(.{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 2 });
        try destination.applyRelationalTopologyControl(.{ .action = .abort_transition, .fence = destination_fence }, null);
        try source.applyRelationalTopologyControl(.{ .action = .abort_transition, .fence = source_fence }, null);
        try std.testing.expect((try destination.relationalTopologyStatus()).fence == null);
        try std.testing.expect((try source.relationalTopologyStatus()).fence == null);
        try std.testing.expectEqualStrings("m", destination.getRange().end);
        return;
    }
    var manifest = try source.relationalHandoffManifest(owned, source_fence, destination_fence, "m", "", 10);
    manifest.merge_copy_attempt = .{ .donor_term = 1, .sequence = 1 };
    try destination.batch(.{ .relational_topology = .{ .action = .transfer, .fence = destination_fence, .transfer = .{ .begin = manifest } } });
    while (true) {
        var read = try destination.core.store.beginProbeTxn();
        const progress = try handoff.loadProgress(owned, &read);
        read.abort();
        if (progress.value.exhausted) break;
        const page = try source.relationalHandoffPage(owned, manifest, progress.value);
        try destination.batch(.{ .relational_topology = .{ .action = .transfer, .fence = destination_fence, .transfer = .{ .page = page } } });
    }
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = manifest.merge_copy_attempt;
    try destination.batchRaftReplicatedApply(.{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 2 });
    var stale = checkpoint;
    stale.copy_attempt.sequence = 0;
    try std.testing.expectError(error.IntegrityHandoffSequenceChanged, destination.batchRaftReplicatedApply(.{ .merge_checkpoint = stale }, .{ .term = 1, .index = 3 }));
    try std.testing.expectError(error.IntegrityHandoffIncomplete, destination.applyRelationalTopologyControl(.{ .action = .prune, .fence = destination_fence }, null));
    if (!rollback) {
        while (true) {
            var read = try destination.core.store.beginProbeTxn();
            const progress = try handoff.loadProgress(owned, &read);
            read.abort();
            if (progress.value.ready) break;
            try destination.batch(.{ .relational_topology = .{ .action = .transfer, .fence = destination_fence, .transfer = .{ .finish = .{ .sequence = progress.value.sequence, .digest = progress.value.digest } } } });
        }
        checkpoint.kind = .bootstrap_complete;
        checkpoint.bootstrap_applied_index = 10;
        try destination.batchRaftReplicatedApply(.{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 4 });
        try std.testing.expectError(error.MergeTransitionNotReady, destination.applyRelationalTopologyControl(.{ .action = .release, .fence = destination_fence }, null));
        checkpoint.kind = .finalize;
        checkpoint.bootstrap_applied_index = 11;
        try destination.batchRaftReplicatedApply(.{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 5 });
        destination.close();
        destination = try db_mod.DB.open(alloc, destination_path, destination_options);
        try destination.batch(.{ .relational_topology = .{ .action = .release, .fence = destination_fence } });
        try destination.batch(.{ .relational_topology = .{ .action = .release, .fence = destination_fence } });
        try std.testing.expect((try destination.relationalTopologyStatus()).fence == null);
        var read = try destination.core.store.beginProbeTxn();
        defer read.abort();
        _ = try read.get(&imported.?.claimKey());
        _ = try read.get(&retained.?.claimKey());
        try std.testing.expectError(error.NotFound, read.get(handoff.manifest_key));
        if (ha) try verifyMergeHAReplay(&primary, replay_start, &standby, imported.?, retained.?, false);
        return;
    }
    checkpoint.kind = .rollback;
    try destination.batchRaftReplicatedApply(.{ .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 4 });
    try std.testing.expectError(error.IntegrityTopologyChanged, destination.applyRelationalTopologyControl(.{ .action = .transfer, .fence = destination_fence, .transfer = .{ .begin = manifest } }, null));
    destination.close();
    destination = try db_mod.DB.open(alloc, destination_path, destination_options);
    var pages: usize = 0;
    while (true) {
        try destination.batch(.{ .relational_topology = .{ .action = .prune, .fence = destination_fence } });
        var read = try destination.core.store.beginProbeTxn();
        defer read.abort();
        const progress = (try std.json.parseFromSlice(handoff.PruneProgress, owned, try read.get(handoff.prune_key), .{})).value;
        pages += 1;
        if (progress.complete) break;
        try std.testing.expectError(error.IntegrityHandoffIncomplete, destination.applyRelationalTopologyControl(.{ .action = .abort_transition, .fence = destination_fence }, null));
    }
    try std.testing.expect(pages > 1);
    try destination.batch(.{ .relational_topology = .{ .action = .abort_transition, .fence = destination_fence } });
    try destination.batch(.{ .relational_topology = .{ .action = .abort_transition, .fence = destination_fence } });
    try std.testing.expect((try destination.relationalTopologyStatus()).fence == null);
    var read = try destination.core.store.beginProbeTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(&imported.?.claimKey()));
    _ = try read.get(&retained.?.claimKey());
    try std.testing.expectError(error.NotFound, read.get(handoff.manifest_key));
    if (ha) try verifyMergeHAReplay(&primary, replay_start, &standby, imported.?, retained.?, true);
}

fn verifyMergeHAReplay(primary: *@import("../hot_standby/primary.zig").Primary, start_lsn: u64, standby: *db_mod.DB, imported: integrity.Address, retained: integrity.Address, rollback: bool) !void {
    var lsn = start_lsn;
    while (lsn <= primary.lastLsn()) : (lsn += 1) {
        var entry = (try primary.log.entryAt(std.testing.allocator, lsn)) orelse return error.TestUnexpectedResult;
        defer entry.deinit(std.testing.allocator);
        try standby.applyHAReplicationRecord(entry.record);
        try standby.applyHAReplicationRecord(entry.record);
    }
    try std.testing.expect((try standby.relationalTopologyStatus()).fence == null);
    try std.testing.expectEqualStrings(if (rollback) "m" else "", standby.getRange().end);
    var read = try standby.core.store.beginProbeTxn();
    defer read.abort();
    _ = try read.get(&retained.claimKey());
    if (rollback) try std.testing.expectError(error.NotFound, read.get(&imported.claimKey())) else _ = try read.get(&imported.claimKey());
    try std.testing.expectError(error.NotFound, read.get(@import("relational_integrity_handoff.zig").manifest_key));
}

test "relational integrity historical restore stays fenced while coherent HA seed preserves namespace" {
    const alloc = std.testing.allocator;
    const lifecycle = @import("generation_lifecycle.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    defer alloc.free(source_path);
    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/target", .{tmp.sub_path});
    defer alloc.free(target_path);
    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/seed", .{source_path});
    defer alloc.free(snapshot_path);
    const namespace = @import("doc_identity.zig").Namespace{ .table_id = 400, .shard_id = 401, .range_id = 402 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } };
    {
        var source = try db_mod.DB.open(alloc, source_path, options);
        defer source.close();
        try source.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"id_unique","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        try std.testing.expectError(error.CoordinatedConstraintTopologyUnsupported, source.split(.{ .start = "", .end = "" }, "m", "", target_path, true));
        try std.testing.expectError(error.CoordinatedConstraintTopologyUnsupported, source.finalizeSplit(.{ .start = "", .end = "m" }));
        _ = try source.snapshot("seed");
    }
    var transition = try lifecycle.beginProcessExclusiveWithRuntime(target_path, null);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    try std.testing.expectError(error.CoordinatedConstraintRestoreRequired, db_mod.DB.restoreSnapshotToStagedGeneration(&staged, alloc, snapshot_path, staged.path(), options));
    var wrong = namespace;
    wrong.table_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, db_mod.DB.restoreCoherentHASeedReplicaToStagedGeneration(&staged, alloc, snapshot_path, staged.path(), options, wrong));
    try db_mod.DB.restoreCoherentHASeedReplicaToStagedGeneration(&staged, alloc, snapshot_path, staged.path(), options, namespace);
    var staged_options = options;
    staged_options.staged_generation = &staged;
    var restored = try db_mod.DB.open(alloc, staged.path(), staged_options);
    defer restored.close();
    try std.testing.expect(restored.core.identity_namespace.eql(namespace));
    _ = try binding(&restored, .unique, "id_unique");
}

test "relational integrity DB activation backfills atomically gates writers and resumes after restart" {
    const alloc = std.testing.allocator;
    const activation = @import("relational_integrity_activation.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/database", .{tmp.sub_path});
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 300, .shard_id = 301 }, .primary_backend = .{ .lsm = .{} } };
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        try db.batch(.{ .timestamp_ns = 100, .writes = &.{ .{ .key = "a", .value = "{\"id\":1}" }, .{ .key = "b", .value = "{\"id\":2}" } } });
        try db.setSchemaJson(alloc,
            \\{"version":2,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"id_unique","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        );
        {
            var oversized = (try activation.Page.prepare(alloc, null, db.core, .{ .output_bytes = 1 })).?;
            defer oversized.deinit();
            try std.testing.expectEqual(.invalid, oversized.progress.state);
            try std.testing.expectEqualStrings("RelationalRowResultTooLarge", oversized.progress.failure);
            // A failed row retains its exact primary observation for the
            // failure CAS, without claiming any successful scan coverage.
            try std.testing.expectEqual(@as(usize, 1), oversized.rows.rows.len);
            try std.testing.expectEqualStrings("a", oversized.rows.rows[0].key);
            try std.testing.expect(oversized.rows.rows[0].expected_content_digest != null);
            try std.testing.expectEqual(@as(usize, 0), oversized.rows.records_examined);
            try std.testing.expectEqual(@as(usize, 0), oversized.progress.cursor.len);
            try std.testing.expectEqual(@as(u64, 0), oversized.progress.rows_scanned);
            const failure_txn = try db.beginTransactionWithId(@splat(50), 200);
            try db.writeTransaction(failure_txn, .{ .relational_schema_version = 2, .relational_activation = oversized.command });
            try db.abortTransaction(failure_txn, 250);
        }
        const blocked = try db.beginTransactionWithId(@splat(51), 300);
        try std.testing.expectError(error.ConstraintActivationInProgress, db.writeTransaction(blocked, .{ .relational_schema_version = 2, .relational_integrity_generation_set = try generationSet(&db), .writes = &.{.{ .key = "c", .value = "{\"id\":3}" }} }));
        try db.abortTransaction(blocked, 400);
        try std.testing.expect(!try activateOnePage(&db, 52));
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        for (53..61) |id| if (try activateOnePage(&db, @intCast(id))) break else {} else return error.ActivationDidNotConverge;
        const raw = try db.core.getStoreValue(alloc, activation.key) orelse return error.MissingActivationProgress;
        defer alloc.free(raw);
        const progress = try activation.Progress.decode(raw);
        try std.testing.expectEqual(.enforced, progress.state);
        try std.testing.expectEqual(@as(u64, 2), progress.rows_scanned);
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var view = db.core.acquireSchemaView().?;
        defer view.release();
        var plan = try tuples.TuplePlan.init(alloc, view.tableSchema().*, view.physicalLayout(), &.{.{ .column = "id" }});
        defer plan.deinit();
        var tuple: std.ArrayList(u8) = .empty;
        defer tuple.deinit(alloc);
        _ = try plan.appendValues(alloc, &tuple, &.{.{ .integer = 1 }});
        const address = try integrity.Address.init(try binding(&db, .unique, "id_unique"), tuple.items);
        try std.testing.expectEqualStrings("a", (try integrity.Claim.decode(&address.claimKey(), try read.get(&address.claimKey()))).parent_key);
    }
}
