// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const abi = @import("kernel_owner_abi");
const kernel_owner_source = @import("../api/kernel_owner_source.zig");
const backup_contract = @import("antfly_local_sources").api_backup_contract;
const db_mod = @import("antfly_source_root").antfly_sources.selected_db;
const distributed_graph = @import("../api/distributed_graph.zig");
const indexes_api = @import("../api/indexes.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const query_api = @import("antfly_local_sources").api_query;
const raft_reconciler = @import("../raft/reconciler.zig");
const read_gate = @import("../raft/read_gate.zig");
const table_catalog = @import("../api/table_catalog.zig");
const table_reads = @import("antfly_source_root").antfly_sources.table_reads;
const table_writes = @import("antfly_source_root").antfly_sources.table_writes;
const shard_state_store = @import("../data/storage/shard_state_store.zig");

test "compiled schema apply control reads retained entries off progress and shuts down" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    const projection_root = try std.fs.path.join(a, &.{ path, "projection" });
    defer a.free(projection_root);
    var store = try @import("data_raft_apply_client.zig").RaftApplyStore.init(a, .{ .root_dir = projection_root });
    defer store.deinit();
    const entries = try @import("../raft/state_machine/mod.zig").encodeCommittedEntries(a, &.{.{ .term = 3, .index = 1, .entry_type = .normal, .data = @constCast("opaque\x00\xff") }});
    defer a.free(entries);
    try store.applyBatch(81, 1, entries);
    const before = (try store.latestBatch(81)).?;
    for ([_]struct { group: u64, term: u64, expected: anyerror }{
        .{ .group = 81, .term = 3, .expected = error.InvalidReplicatedBatch },
        .{ .group = 81, .term = 4, .expected = error.ConflictingDataApplyBatch },
        .{ .group = 82, .term = 3, .expected = error.SchemaValidationRuntimeRecoveryRequired },
    }) |scenario| {
        // No catalog or owner exists. Invalid/missing retained evidence must
        // never become an empty successful apply or trigger catalog lookup.
        var source = kernel_owner_source.ProvisionedKernelOwnerSource.init(a, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        _ = source.withSchemaEntryStore(&store);
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.applyPreparedSchemaEntry(scenario.group, scenario.term, 1));
        try source.startApplyControl(std.testing.io);
        const limit = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (true) {
            source.applyPreparedSchemaEntry(scenario.group, scenario.term, 1) catch |err| {
                if (err == error.RaftApplyWriterUnavailable) {
                    if (@import("antfly_platform").time.monotonicNs() >= limit) return error.TestSchemaControlDidNotComplete;
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                    continue;
                }
                try std.testing.expectEqual(scenario.expected, err);
                break;
            };
            return error.TestExpectedError;
        }
        source.retireSchemaApplyGroup(scenario.group);
        try std.testing.expectEqual(before, (try store.latestBatch(81)).?);
    }
}

test "native index owner adapter inspects cold native ownership and rejects foreign namespaces" {
    const alloc = std.testing.allocator;
    const local = @import("antfly_local_sources");
    var checkpoint: []const u8 = "native ownership";
    errdefer std.debug.print("owner source provenance checkpoint={s}\n", .{checkpoint});
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const Catalog = struct {
        fn snapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_incarnation = "61616161616161616161616161616161".*, .metrics = .{} },
                .tables = @constCast(&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 101,
                    .name = "items",
                    .schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}",
                }}),
                .ranges = @constCast(&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7501, .table_id = 101, .range_id = 7501, .start_key = "", .end_key = null }}),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn validate(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
            var current = try snapshot(ptr);
            return contract.matches(&current);
        }
        fn source(self: *@This()) table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{
                .admin_snapshot = snapshot,
                .free_admin_snapshot = free,
                .linearizable_routing_snapshot = table_catalog.TestAdminRoutingAdapter(snapshot, free).linearizableSnapshot,
                .free_routing_snapshot = table_catalog.TestAdminRoutingAdapter(snapshot, free).freeRoutingSnapshot,
                .validate_publication = validate,
            } };
        }
    };
    var catalog = Catalog{};
    const identity: local.storage_db_relational_index_owner_control.Identity = .{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner_digest = @splat(3),
        .ordinal = 0,
        .namespace = .{ .table_id = 101, .shard_id = 7501, .range_id = 7501 },
        .owner_group_id = 7501,
    };
    const schema_binding: local.storage_db_relational_nullability_job.Binding = .{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner = @splat(3),
        .fence = .{ .transition_id = 4, .attempt = 1, .admission_epoch = 1, .owner_group_id = 7501, .peer_group_id = 7501, .namespace = identity.namespace, .catalog_digest = @splat(5), .role = .schema_validation },
        .authority = .native,
        .before_version = 1,
        .after_version = 2,
        .columns = 1,
        .before_schema = @splat(6),
        .after_schema = @splat(7),
        .before_public = @splat(8),
        .after_public = @splat(9),
        .after_catalog = @splat(10),
        .physical_catalog = @splat(11),
        .inputs = @splat(12),
        .projection_identity = @splat(13),
    };
    var physical_root: u128 = 0;
    const SchemaRootGeneration = struct {
        fn read(ptr: *anyopaque, _: u64) u64 {
            return @as(*const u64, @ptrCast(@alignCast(ptr))).*;
        }
    };
    var accepted_root_generation: u64 = 11;
    // Repeat from a fully closed source: neither the decoded status nor the
    // native mode proof may depend on a retained process-local job or lease.
    for (0..2) |_| {
        var owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, catalog.source(), read_gate.alreadyReadSafeBarrier());
        owners.online_source_authority = .native;
        owners.group_visible_root_generation = .{ .ptr = &accepted_root_generation, .visible_root_generation_for_group = SchemaRootGeneration.read };
        defer owners.deinit();
        var state = try owners.nativeIndexOwnerState(alloc, 7501, "items", identity, .{});
        defer state.deinit();
        try std.testing.expect(state.value == null);
        var receipt = try owners.nativeIndexOwnerReceipt(alloc, 7501, "items", .{ .identity = identity, .effect = .fenced }, .{});
        defer receipt.deinit();
        try std.testing.expect(receipt.value == null);
        var rooted = try owners.nativeIndexOwnerReceiptAtRoot(alloc, 7501, "items", .{ .identity = identity, .effect = .fenced }, .{});
        defer rooted.deinit();
        try std.testing.expect(rooted.outcome.value == null);
        try std.testing.expectEqual(try local.storage_db_root_signing_identity.attestedRootGeneration(accepted_root_generation), rooted.root_generation);
        const schema = try owners.nativeSchemaOwnerObservationAtRoot(7501, "items", schema_binding, .{});
        try schema.observation.validateFor(schema_binding);
        try std.testing.expect(schema.observation.header == null and !schema.observation.replication_pending);
        try std.testing.expectEqual(try local.storage_db_root_signing_identity.attestedRootGeneration(accepted_root_generation), schema.root_generation);
        if (physical_root != 0) try std.testing.expectEqual(physical_root, schema.observation.physical_root_incarnation);
        physical_root = schema.observation.physical_root_incarnation;
        // Valid synthetic signing data deliberately cannot turn a real
        // empty owner observation into a receipt. This is not enrollment or
        // positive metadata/publication evidence.
        const attestation = @import("../metadata/schema_validation_attestation.zig");
        const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(9));
        const signing_root: local.storage_db_root_signing_identity.State = .{ .seed = @splat(9), .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
        const signing_identity: @import("../metadata/store_root_enrollment.zig").Identity = .{
            .metadata_incarnation = "61616161616161616161616161616161".*,
            .node_id = 2,
            .store_id = 3,
            .root_incarnation = signing_root.root_incarnation,
            .public_key = signing_root.public_key,
        };
        const native_binding: @import("../metadata/standalone_native_owner.zig").Binding = .{
            .metadata_incarnation = signing_identity.metadata_incarnation,
            .node_id = signing_identity.node_id,
            .store_id = signing_identity.store_id,
            .root_incarnation = signing_identity.root_incarnation,
        };
        const signing_scope: attestation.NativeScope = .{
            .metadata_incarnation = signing_identity.metadata_incarnation,
            .binding_digest = try attestation.bindingDigest(schema_binding),
            .signing = .{ .identity = signing_identity, .reporter_incarnation = 4, .binding_digest = try native_binding.digest() },
            .root_generation = schema.root_generation,
            .physical_root_incarnation = schema.observation.physical_root_incarnation,
        };
        try std.testing.expectError(error.SchemaValidationJobRequired, owners.attestNativeSchemaOwnerAtRoot(7501, "items", schema_binding, signing_scope, signing_root, .{}));
        var stale_scope = signing_scope;
        stale_scope.physical_root_incarnation += 1;
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, owners.attestNativeSchemaOwnerAtRoot(7501, "items", schema_binding, stale_scope, signing_root, .{}));
        const cancel: local.storage_db_relational_schema_owner_contract.Command = .{ .binding = schema_binding, .action = .cancel };
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, owners.applyNativeSchemaOwnerControlAtRoot(7501, "items", cancel, 1, std.time.ns_per_s, .{ .root_generation = schema.root_generation + 1, .physical_root_incarnation = schema.observation.physical_root_incarnation }, .{}));
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, owners.applyNativeSchemaOwnerControlAtRoot(7501, "items", cancel, 1, std.time.ns_per_s, .{ .root_generation = schema.root_generation, .physical_root_incarnation = schema.observation.physical_root_incarnation + 1 }, .{}));
        try std.testing.expectEqualDeep(schema, try owners.nativeSchemaOwnerObservationAtRoot(7501, "items", schema_binding, .{}));
        var foreign_schema = schema_binding;
        foreign_schema.fence.namespace.range_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, owners.nativeSchemaOwnerObservationAtRoot(7501, "items", foreign_schema, .{}));
        try std.testing.expectError(error.OnlineSourceScopeChanged, owners.raftIndexOwnerObservationAtRoot(alloc, 7501, "items", identity, .{}));
        var foreign = identity;
        foreign.namespace.range_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, owners.nativeIndexOwnerState(alloc, 7501, "items", foreign, .{}));
        try std.testing.expectError(error.IdentityNamespaceMismatch, owners.nativeIndexOwnerReceiptAtRoot(alloc, 7501, "items", .{ .identity = foreign, .effect = .fenced }, .{}));
        var after_rejection = try owners.nativeIndexOwnerState(alloc, 7501, "items", identity, .{});
        defer after_rejection.deinit();
        try std.testing.expect(after_rejection.value == null);
    }
    // A separate durable Raft domain exercises the real leased preparation
    // bridge. These supplied catalog/header inputs are not metadata authority.
    const raft_root = try std.fmt.allocPrint(alloc, "{s}/raft-owner", .{root});
    defer alloc.free(raft_root);
    var raft_owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, raft_root, catalog.source(), read_gate.alreadyReadSafeBarrier());
    defer raft_owners.deinit();
    const empty = try raft_owners.raftIndexOwnerObservationAtRoot(alloc, 7501, "items", identity, .{});
    var raft_schema_binding = schema_binding;
    raft_schema_binding.authority = .raft;
    const empty_schema = try raft_owners.raftSchemaOwnerObservationAtRoot(7501, "items", raft_schema_binding, .{});
    try std.testing.expect(empty_schema.observation.header == null and empty_schema.observation.applied == null);
    try std.testing.expect(empty_schema.observation.physical_root_incarnation != 0);
    try std.testing.expectEqual(empty.root_generation, empty_schema.root_generation);
    try std.testing.expectError(error.SchemaValidationJobChanged, raft_owners.raftSchemaOwnerObservationAtRoot(7501, "items", schema_binding, .{}));
    try std.testing.expectError(error.SchemaValidationJobChanged, raft_owners.acquireRaftSchemaOwnerSession(7502, "items", raft_schema_binding, .{}));
    var foreign_raft_schema = raft_schema_binding;
    foreign_raft_schema.fence.namespace.range_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, raft_owners.acquireRaftSchemaOwnerSession(7501, "items", foreign_raft_schema, .{}));
    {
        var canceled = std.atomic.Value(bool).init(false);
        const context: local.api_operation.RequestContext = .{ .cancellation = local.api_operation.CancellationToken.fromAtomic(&canceled) };
        var schema_session = try raft_owners.acquireRaftSchemaOwnerSession(7501, "items", raft_schema_binding, context);
        defer schema_session.deinit();
        try std.testing.expectEqualDeep(empty_schema, schema_session.initial);
        try std.testing.expectEqualDeep(empty_schema, try schema_session.observe());
        schema_session.releaseForApply();
        try std.testing.expectEqual(@as(usize, 1), raft_owners.retireTable("items"));
        try schema_session.reacquireAfterApply();
        try std.testing.expectEqualDeep(empty_schema, try schema_session.recoverCommitted());
        canceled.store(true, .release);
        try std.testing.expectError(error.Canceled, schema_session.observe());
        // An already-leased read of durable facts survives request cancellation;
        // it does not mint an entry/receipt for this actually empty Raft owner.
        try std.testing.expectEqualDeep(empty_schema, try schema_session.recoverCommitted());
        schema_session.releaseForApply();
        try std.testing.expectError(error.Canceled, schema_session.reacquireAfterApply());
        canceled.store(false, .release);
        // Exercise the provenance guards independently. These altered retained
        // identities are negative fixtures, not evidence of root adoption.
        schema_session.initial.root_generation += 1;
        checkpoint = "empty owner retained generation mismatch";
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, schema_session.reacquireAfterApply());
        schema_session.initial.root_generation -= 1;
        schema_session.initial.observation.physical_root_incarnation += 1;
        checkpoint = "empty owner retained physical root mismatch";
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, schema_session.reacquireAfterApply());
        schema_session.initial.observation.physical_root_incarnation -= 1;
        try schema_session.reacquireAfterApply();
        try std.testing.expectEqualDeep(empty_schema, try schema_session.observe());
        const attestation = @import("../metadata/schema_validation_attestation.zig");
        const seed: [32]u8 = @splat(9);
        const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
        const signing_root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
        const scope: attestation.RaftScope = .{
            .metadata_incarnation = "61616161616161616161616161616161".*,
            .binding_digest = try attestation.bindingDigest(raft_schema_binding),
            .identity = .{ .metadata_incarnation = "61616161616161616161616161616161".*, .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = signing_root.public_key },
            .reporter_incarnation = 4,
            .replica_id = 5,
            .group_id = 7501,
            .root_generation = empty_schema.root_generation,
            .physical_root_incarnation = empty_schema.observation.physical_root_incarnation,
            .reported_term = 1,
            .reported_applied_index = 1,
        };
        try std.testing.expectError(error.InvalidControlReceiptPosition, schema_session.attest(scope, signing_root));
        try std.testing.expectError(error.InvalidControlReceiptPosition, raft_owners.attestRaftSchemaOwnerAtRoot(7501, "items", raft_schema_binding, scope, signing_root, .{}));
    }
    try std.testing.expectError(error.OnlineSourceScopeChanged, raft_owners.nativeSchemaOwnerObservationAtRoot(7501, "items", schema_binding, .{}));
    try std.testing.expect(empty.observation.binding == null and empty.observation.applied == null);
    try std.testing.expectEqual(try local.storage_db_root_signing_identity.attestedRootGeneration(table_reads.backend_current_root_generation), empty.root_generation);
    const before_json = (try Catalog.snapshot(&catalog)).tables[0].schema_json;
    const after_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}},"relational_indexes":[{"name":"id_idx","keys":[{"column":"id"}]}],"unique_constraints":[{"name":"id_idx","origin":"index","columns":["id"]}]}
    ;
    var schema = try local.schema_mod.parseValidatedTableSchema(alloc, before_json);
    defer schema.deinit(alloc);
    var prior = try local.schema_relational_declarations.compileCatalog(alloc, schema, 101, null);
    defer prior.deinit();
    var candidate = try local.storage_db_relational_index_candidate.Candidate.prepare(alloc, 101, before_json, after_json, prior.value, "id_idx");
    defer candidate.deinit();
    const header: local.storage_db_relational_index_raft_contract.Admission = .{ .id = identity.id, .plan_digest = identity.plan_digest, .owner_digest = identity.owner_digest, .ordinal = identity.ordinal, .fence = .{ .role = .index_build, .transition_id = 9, .attempt = 1, .owner_group_id = 7501, .peer_group_id = 7501, .namespace = identity.namespace, .catalog_digest = local.storage_db_relational_index_inputs.digest(prior.value) }, .candidate_digest = local.storage_db_relational_index_inputs.digest(candidate.next.value), .reservation_digest = local.storage_db_relational_index_inputs.digest(candidate.reservation.value) };
    const prepared = try raft_owners.raftIndexOwnerPreparationAtRoot(alloc, 7501, "items", header, candidate.inputs.bytes, .{});
    try prepared.preparation.validate(header, local.storage_db_relational_index_inputs.digest(candidate.inputs.bytes));
    try std.testing.expectEqual(empty.root_generation, prepared.root_generation);
    try std.testing.expectEqualDeep(empty, try raft_owners.raftIndexOwnerObservationAtRoot(alloc, 7501, "items", identity, .{}));
    // A preparation lease must not prevent the control executor from retiring
    // the resident entry before committed-descriptor apply. Rejoin the same
    // physical root through the real compiled owner, never reuse its pointer.
    var session = try raft_owners.acquireRaftIndexOwnerSession(7501, "items", identity, .{});
    defer session.deinit();
    session.releaseForApply();
    try std.testing.expectEqual(@as(usize, 1), raft_owners.retireTable("items"));
    try session.reacquireAfterApply();
    try std.testing.expectEqualDeep(empty, try session.observe(alloc));
    session.releaseForApply();
    session.root_generation += 1;
    try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, session.reacquireAfterApply());
    try std.testing.expect(!session.lease.active);
    session.root_generation -= 1;
    try session.reacquireAfterApply();
    try std.testing.expectEqualDeep(empty, try session.observe(alloc));

    // The trusted committed-entry driver below exercises the real prepared
    // schema ticket, not metadata admission or fabricated publication authority.
    // Its ticket retains the owner even after the submitting session releases.
    {
        const schema_root = try std.fs.path.join(alloc, &.{ root, "schema-ticket" });
        defer alloc.free(schema_root);
        const seed_path = try std.fs.path.join(alloc, &.{ schema_root, "group-7501", "table-db" });
        defer alloc.free(seed_path);
        const tightened =
            \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer","nullable":false}},"required":["id"],"additionalProperties":false}}}}
        ;
        const inputs = try local.storage_db_relational_schema_owner_contract.encodeInputs(alloc, .{ before_json, tightened, prior.value, "[\"id\"]" });
        defer alloc.free(inputs);
        const actual_binding = binding: {
            var seed = try @import("kernel_owner_client.zig").Owner.open(.{ .path = .fromSlice(seed_path), .table_name = .fromSlice("items"), .group_id = 7501, .lsm_root_generation = table_reads.backend_current_root_generation, .has_identity_namespace = 1, .identity_table_id = 101, .identity_shard_id = 7501, .identity_range_id = 7501, .online_source_authority = 1, .schema_json = .fromSlice(before_json), .indexes_json = .fromSlice("{}") });
            defer seed.deinit();
            const lookup = try table_reads.encodeStorageKernelLookupRequest(alloc, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" });
            defer alloc.free(lookup);
            var observed = try seed.lookupJson("items", lookup);
            defer observed.deinit();
            var topology = try std.json.parseFromSlice(local.storage_db_relational_integrity_topology_contract.Identity, alloc, observed.bytes(), .{});
            defer topology.deinit();
            const schema_prepared_cut = try seed.schemaOwnerPreparation("items", .{ .id = @splat(21), .provisional_plan = @splat(22), .fence = .{ .role = .schema_validation, .transition_id = 23, .attempt = 1, .admission_epoch = topology.value.next_epoch, .owner_group_id = 7501, .peer_group_id = 7501, .namespace = identity.namespace, .catalog_digest = topology.value.catalog_digest }, .authority = .raft }, inputs, .{});
            break :binding schema_prepared_cut.binding;
        };
        var source = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, schema_root, catalog.source(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        var canceled = std.atomic.Value(bool).init(false);
        var control = try source.acquireRaftSchemaOwnerSession(7501, "items", actual_binding, .{ .cancellation = local.api_operation.CancellationToken.fromAtomic(&canceled) });
        defer control.deinit();
        const bootstrap_root = try local.storage_db_root_signing_identity.loadOrCreate(alloc, std.testing.io, schema_root);
        const bootstrap_scope: @import("../metadata/schema_validation_attestation.zig").RaftScope = .{
            .metadata_incarnation = "61616161616161616161616161616161".*,
            .binding_digest = try @import("../metadata/schema_validation_attestation.zig").bindingDigest(actual_binding),
            .identity = .{ .metadata_incarnation = "61616161616161616161616161616161".*, .node_id = 2, .store_id = 3, .root_incarnation = bootstrap_root.root_incarnation, .public_key = bootstrap_root.public_key },
            .reporter_incarnation = 4,
            .replica_id = 5,
            .group_id = 7501,
            .root_generation = control.initial.root_generation,
            .physical_root_incarnation = control.initial.observation.physical_root_incarnation,
            .reported_term = 2,
            .reported_applied_index = 2,
        };
        const bootstrap_descriptor: local.storage_db_relational_schema_owner_contract.PreparationDescriptor = .{ .id = actual_binding.id, .provisional_plan = actual_binding.plan_digest, .fence = actual_binding.fence, .authority = .raft };
        // The native preparation has no data-entry clock/header. The runtime
        // barrier is a fixture here, not a simulated Raft quorum; production
        // wiring uses DataServer's actual shared-prefix certifier.
        try std.testing.expect(control.initial.observation.applied == null);
        try std.testing.expectError(error.ReadSafetyBarrierUnavailable, source.attestSchemaBootstrapAtRoot(7501, "items", bootstrap_descriptor, inputs, bootstrap_scope, bootstrap_root, .{}));
        const BootstrapBarrier = struct {
            calls: usize = 0,
            fn wait(_: *anyopaque, _: u64, _: []const u8) !void {}
            fn certify(ptr: *anyopaque, group: u64, term: u64, minimum: u64, _: ?u64, _: local.api_operation.CancellationToken) !read_gate.ReadSafetyBarrier.FrozenProof {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(@as(u64, 7501), group);
                try std.testing.expectEqual(@as(u64, 2), term);
                try std.testing.expectEqual(@as(u64, 2), minimum);
                self.calls += 1;
                return .{ .incarnation = 91, .term = 2, .applied_index = 3 };
            }
        };
        var bootstrap_barrier: BootstrapBarrier = .{};
        const original_barrier = source.read_safety_barrier;
        source.read_safety_barrier = .{ .ptr = &bootstrap_barrier, .vtable = &.{ .wait_read_safe = BootstrapBarrier.wait, .certify_completed_prefix = BootstrapBarrier.certify } };
        const certified = try source.attestSchemaBootstrapAtRoot(7501, "items", bootstrap_descriptor, inputs, bootstrap_scope, bootstrap_root, .{});
        try certified.verify(bootstrap_scope);
        try std.testing.expect(certified.preparation.value.position == null);
        try std.testing.expectEqual(@as(usize, 1), bootstrap_barrier.calls);
        source.read_safety_barrier = original_barrier;
        {
            const before_capture = try source.schemaOwnerStateAtRoot(7501, "items", .{});
            var captured = try source.captureSchemaPreparationAtRoot(alloc, 7501, "items", .{ .id = actual_binding.id, .provisional_plan = actual_binding.plan_digest, .transition_id = actual_binding.fence.transition_id, .namespace = actual_binding.fence.namespace }, before_json, tightened, &.{"id"}, .{ .identity = bootstrap_scope.identity, .reporter_incarnation = bootstrap_scope.reporter_incarnation, .authority = .{ .raft = bootstrap_scope.replica_id } }, bootstrap_root, .{});
            defer captured.deinit();
            try std.testing.expectEqualSlices(u8, inputs, captured.inputs);
            try std.testing.expectEqualDeep(actual_binding, captured.signed.value.binding);
            try std.testing.expect(captured.signed.value.position == null);
            try captured.signed.verifyFor(actual_binding, before_capture.state.physical_root_incarnation);
            try std.testing.expectEqualDeep(before_capture, try source.schemaOwnerStateAtRoot(7501, "items", .{}));
            {
                const builder = @import("../api/schema_validation_plan_builder.zig");
                const manager = @import("../metadata/table_manager.zig");
                const before = (try Catalog.snapshot(&catalog)).tables[0];
                const after = try @import("../api/tables.zig").prepareNullabilityPublicationRecord(alloc, &before, &.{"id"});
                defer manager.freeTable(alloc, after);
                const guard: local.system_catalog_domain.RelationMutationGuard = .{ .target = .{ .name = "items" }, .logical_table = "items", .owner = .{ .table_id = 101, .schema_version = 1, .schema_digest = local.storage_db_relational_schema_owner_contract.hash(before.schema_json), .kind = .table }, .incarnation = @splat(0x61) };
                const cut: @import("../metadata/schema_validation_admission.zig").Cut = .{ .metadata_group_id = 21, .metadata_incarnation = bootstrap_scope.metadata_incarnation, .table_guard = guard, .before = before, .after = after, .ranges = &.{.{ .range_id = 7501, .table_id = 101, .group_id = 7501, .start_key = "", .end_key = null }}, .authority = .raft };
                const Owners = struct {
                    source: *kernel_owner_source.ProvisionedKernelOwnerSource,
                    signer: kernel_owner_source.ProvisionedKernelOwnerSource.SchemaPreparationSigner,
                    root: local.storage_db_root_signing_identity.State,
                    calls: usize = 0,
                    pub fn prepare(self: *@This(), a: std.mem.Allocator, group: u64, table: []const u8, query: @import("../api/schema_validation_preparation.zig").Request, context: local.api_operation.RequestContext) ![]u8 {
                        self.calls += 1;
                        var value = try self.source.captureSchemaPreparationAtRoot(a, group, table, .{ .id = query.id, .provisional_plan = query.provisional_plan, .transition_id = query.transition_id, .namespace = query.namespace }, query.before_schema, query.after_schema, query.columns, self.signer, self.root, context);
                        defer value.deinit();
                        return @import("../api/schema_validation_preparation.zig").encode(a, value.signed, value.inputs);
                    }
                };
                var actual_owners: Owners = .{ .source = &source, .signer = .{ .identity = bootstrap_scope.identity, .reporter_incarnation = bootstrap_scope.reporter_incarnation, .authority = .{ .raft = bootstrap_scope.replica_id } }, .root = bootstrap_root };
                const context: local.api_operation.RequestContext = .{ .setting_admin = true, .schema_validation_authority = true };
                var built = try builder.build(&actual_owners, alloc, std.testing.io, context, cut, .{ .guard = guard, .columns = &.{"id"} });
                defer built.deinit();
                try std.testing.expectEqual(@as(usize, 1), actual_owners.calls);
                try std.testing.expectEqualDeep(before_capture, try source.schemaOwnerStateAtRoot(7501, "items", .{}));
                var command = try @import("../metadata/schema_validation_ledger.zig").parseCommand(alloc, built.command);
                defer command.deinit();
                try std.testing.expectEqual(@as(usize, 1), command.value.begin.inputs.len);
                try std.testing.expectEqual(@as(usize, 1), command.value.begin.preparations.len);
                try command.value.begin.preparations[0].verifyFor(built.prepared.owners[0].binding, before_capture.state.physical_root_incarnation);
                try std.testing.expectEqual(built.lookup.plan_digest, try command.value.begin.plan.digest());
                try std.testing.expectEqualDeep(built.lookup, try @import("../api/schema_validation_recovery.zig").Token.decode(&built.recovery_token));
                try std.testing.expect(command.value.begin.plan.before.schema_json.ptr != before.schema_json.ptr);
                try std.testing.expectError(error.Forbidden, builder.build(&actual_owners, alloc, std.testing.io, .{}, cut, .{ .guard = guard, .columns = &.{"id"} }));
                try std.testing.expectEqual(@as(usize, 1), actual_owners.calls);
                try std.testing.expectError(error.DeadlineExceeded, builder.build(&actual_owners, alloc, std.testing.io, .{ .setting_admin = true, .schema_validation_authority = true, .deadline_ns = 0 }, cut, .{ .guard = guard, .columns = &.{"id"} }));
                var foreign = cut;
                foreign.authority = .native;
                try std.testing.expectError(error.SchemaValidationJobChanged, builder.build(&actual_owners, alloc, std.testing.io, context, foreign, .{ .guard = guard, .columns = &.{"id"} }));
                try std.testing.expectEqualDeep(before_capture, try source.schemaOwnerStateAtRoot(7501, "items", .{}));
            }
            const transport = @import("../api/schema_validation_preparation.zig");
            var request: transport.Request = .{ .group_id = 7501, .metadata_incarnation = bootstrap_scope.metadata_incarnation, .id = actual_binding.id, .provisional_plan = actual_binding.plan_digest, .transition_id = actual_binding.fence.transition_id, .namespace = actual_binding.fence.namespace, .before_schema = before_json, .after_schema = tightened, .columns = &.{"id"} };
            const frame = try transport.encode(alloc, captured.signed, captured.inputs);
            defer alloc.free(frame);
            const decoded = try transport.decode(frame);
            try decoded.requireFor(7501, request);
            try std.testing.expectEqualSlices(u8, captured.inputs, decoded.inputs);
            const Http = @import("../raft/transport/http_common.zig");
            const Executor = struct {
                frame: []const u8,
                calls: usize = 0,
                mode: enum { valid, wrong_media, lost_reply } = .valid,
                fn execute(ptr: *anyopaque, a: std.mem.Allocator, req: Http.HttpRequest) !Http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.calls += 1;
                    try std.testing.expectEqual(Http.Method.POST, req.method);
                    try std.testing.expect(std.mem.endsWith(u8, req.uri, "/schema-validation-prepare"));
                    try std.testing.expect(std.mem.indexOf(u8, req.uri, "items%2Fquoted") != null);
                    try std.testing.expect(req.header(@import("../api/internal_service_auth.zig").header_name) != null);
                    try std.testing.expect(req.header(transport.remaining_ms_header) != null);
                    try std.testing.expectEqual(@as(?usize, transport.max_response_bytes), req.max_response_bytes);
                    if (self.mode == .lost_reply) return error.ConnectionResetByPeer;
                    const mime = try a.dupe(u8, if (self.mode == .wrong_media) "text/plain" else "application/octet-stream");
                    errdefer a.free(mime);
                    return .{ .status = 200, .content_type = mime, .body = try a.dupe(u8, self.frame) };
                }
            };
            var executor: Executor = .{ .frame = frame };
            var http = @import("../api/http_client.zig").ApiHttpClient.init(alloc, .{ .ptr = &executor, .vtable = &.{ .execute = Executor.execute } });
            _ = http.withInternalServiceAuth("schema-preparation-test-secret", "schema-test");
            {
                var response = try http.fetchSchemaValidationPreparation("http://owner", 7501, "items/quoted", request, .{});
                defer response.deinit();
                try std.testing.expectEqualSlices(u8, captured.inputs, response.value.inputs);
                try std.testing.expectEqualDeep(captured.signed, response.value.signed);
            }
            executor.mode = .wrong_media;
            try std.testing.expectError(error.InvalidSchemaValidationJob, http.fetchSchemaValidationPreparation("http://owner", 7501, "items/quoted", request, .{}));
            const before_loss = executor.calls;
            executor.mode = .lost_reply;
            try std.testing.expectError(error.ConnectionResetByPeer, http.fetchSchemaValidationPreparation("http://owner", 7501, "items/quoted", request, .{}));
            try std.testing.expectEqual(before_loss + 1, executor.calls);
            try std.testing.expectError(error.OutOfMemory, transport.encode(std.testing.failing_allocator, captured.signed, captured.inputs));
            frame[frame.len - 1] ^= 1;
            try std.testing.expectError(error.InvalidSchemaValidationJob, transport.decode(frame));
            frame[frame.len - 1] ^= 1;
            const omitted = try transport.encode(alloc, captured.signed, "");
            defer alloc.free(omitted);
            const compact = try transport.decode(omitted);
            try std.testing.expectError(error.SchemaValidationJobChanged, compact.requireFor(7501, request));
            const known = [_][32]u8{actual_binding.inputs};
            request.known_inputs = &known;
            try compact.requireFor(7501, request);
            try std.testing.expect(omitted.len < frame.len);
            request.before_schema = "{}";
            try std.testing.expectError(error.SchemaValidationJobChanged, compact.requireFor(7501, request));
            // Cohort commitment is formed AFTER these physical preparations.
            // The established signature contract deliberately normalizes only
            // that provisional digest, avoiding a second native compilation.
            var final_binding = actual_binding;
            final_binding.plan_digest = @splat(121);
            try captured.signed.verifyFor(final_binding, before_capture.state.physical_root_incarnation);
            final_binding.after_schema[0] ^= 1;
            try std.testing.expectError(error.SchemaValidationJobChanged, captured.signed.verifyFor(final_binding, before_capture.state.physical_root_incarnation));
        }
        const command: local.storage_db_relational_schema_owner_contract.Command = .{ .binding = actual_binding, .action = .begin, .payload = inputs };
        var foreign_command = command;
        foreign_command.binding.id = @splat(24);
        const foreign_bytes = try foreign_command.encodeRaft(alloc);
        defer alloc.free(foreign_bytes);
        try std.testing.expectError(error.SchemaValidationJobChanged, control.prepareCommitted(foreign_bytes, 1, 1));
        const bytes = try command.encodeRaft(alloc);
        var bytes_live = true;
        defer if (bytes_live) alloc.free(bytes);
        var ticket = try control.prepareCommitted(bytes, 1, 1);
        var ticket_live = true;
        defer if (ticket_live) ticket.deinit();
        try std.testing.expectError(error.StorageBusy, control.prepareCommitted(bytes, 1, 1));
        alloc.free(bytes); // no borrowed committed-entry payload in the ticket
        bytes_live = false;
        control.releaseForApply();
        canceled.store(true, .release);
        try std.testing.expectError(error.DeadlineExceeded, ticket.apply(.{ .execution_deadline_ns = 0 }));
        try std.testing.expect((try ticket.observeCommitted()).observation.header == null);
        try ticket.apply(.{});
        try ticket.apply(.{}); // exact durable term/index retry
        const applied = try ticket.observeCommitted();
        try std.testing.expectEqualDeep(actual_binding, applied.observation.header.?.binding);
        try std.testing.expectEqual(.fencing, applied.observation.header.?.phase);
        try std.testing.expectEqual(@as(u64, 1), applied.observation.applied.?.index);
        try std.testing.expectEqual(@as(u64, 1), applied.observation.applied.?.term);
        try std.testing.expectEqual(control.initial.observation.physical_root_incarnation, applied.observation.physical_root_incarnation);
        ticket.deinit();
        ticket_live = false;
        // BEGIN has durably installed its topology fence. Preparation must
        // refuse that fence before it could certify an empty/beginning owner.
        try std.testing.expectError(error.IntegrityTopologyBusy, source.attestSchemaBootstrapAtRoot(7501, "items", bootstrap_descriptor, inputs, bootstrap_scope, bootstrap_root, .{}));
        // The preparation is destroyed before its owner can retire. Reopen
        // independently and recover the same real phase/entry/root, not a flag.
        try std.testing.expectEqual(@as(usize, 1), source.retireTable("items"));
        canceled.store(false, .release);
        try control.reacquireAfterApply();
        try std.testing.expectEqualDeep(applied, try control.recoverCommitted());
        {
            var proposal = try control.prepareProposal(.reconcile_transactions, 1, std.time.ns_per_s);
            defer proposal.deinit();
            try std.testing.expectEqual(applied.observation.physical_root_incarnation, proposal.physical_root_incarnation);
            try std.testing.expectEqualDeep(applied, try control.recoverCommitted());
            control.initial.root_generation += 1;
            try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, control.prepareProposal(.reconcile_transactions, 1, std.time.ns_per_s));
            control.initial.root_generation -= 1;
        }
        const recovered_state = try source.schemaOwnerStateAtRoot(7501, "items", .{});
        try std.testing.expectEqual(applied.root_generation, recovered_state.root_generation);
        try std.testing.expectEqual(applied.observation.physical_root_incarnation, recovered_state.state.physical_root_incarnation);
        try std.testing.expectEqualDeep(applied.observation.header, recovered_state.state.header);
        try std.testing.expectEqualDeep(actual_binding.fence.namespace, recovered_state.state.namespace);
        {
            const audit_key = try local.storage_db_root_signing_identity.loadOrCreate(alloc, std.testing.io, schema_root);
            // The runtime discovers a cold owner before it knows the job and
            // probes its native continuation before downloading immutable ASVI.
            const cold_probe = try source.stepSchemaOwnerAuditAtRoot(7501, "items", actual_binding, "", audit_key, .{ .records = 1 }, .{});
            try std.testing.expectEqual(.needs_inputs, std.meta.activeTag(cold_probe.result));
            try std.testing.expectEqualDeep(applied, try control.recoverCommitted());
            try std.testing.expectError(error.InvalidSchemaValidationJob, source.stepSchemaOwnerAuditAtRoot(7501, "items", actual_binding, "wrong-inputs", audit_key, .{}, .{}));
            const changed_inputs = try local.storage_db_relational_schema_owner_contract.encodeInputs(alloc, .{ before_json, tightened, "different-catalog", "[\"id\"]" });
            defer alloc.free(changed_inputs);
            try std.testing.expectError(error.SchemaValidationJobChanged, source.stepSchemaOwnerAuditAtRoot(7501, "items", actual_binding, changed_inputs, audit_key, .{}, .{}));
            var audited = false;
            for (0..64) |iteration| {
                // All later slices borrow the DB-owned snapshot/input state;
                // no request-owned input bytes survive the initial call.
                const turn = try source.stepSchemaOwnerAuditAtRoot(7501, "items", actual_binding, if (iteration == 0) inputs else "", audit_key, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
                try std.testing.expectEqual(applied.root_generation, turn.root_generation);
                try std.testing.expect(turn.result != .needs_inputs);
                if (turn.result == .complete) {
                    const image = turn.result.complete;
                    try image.verify(audit_key.public_key);
                    try std.testing.expectEqual(applied.observation.physical_root_incarnation, image.value.physical_root);
                    try std.testing.expectEqualDeep(applied.observation.header.?, image.value.rows.header);
                    try std.testing.expectEqual(.none, image.value.rows.coverage);
                    audited = true;
                    break;
                }
            }
            try std.testing.expect(audited);
            try std.testing.expectEqualDeep(applied, try control.recoverCommitted());
            // Cancellation cannot turn a resident continuation into authority,
            // nor may it destroy progress needed by a subsequent clean slice.
            canceled.store(true, .release);
            try std.testing.expectError(error.Canceled, source.schemaOwnerStateAtRoot(7501, "items", .{ .cancellation = .fromAtomic(&canceled) }));
            try std.testing.expectError(error.Canceled, source.stepSchemaOwnerAuditAtRoot(7501, "items", actual_binding, "", audit_key, .{}, .{ .cancellation = .fromAtomic(&canceled) }));
            canceled.store(false, .release);
            const resumed = try source.stepSchemaOwnerAuditAtRoot(7501, "items", actual_binding, "", audit_key, .{}, .{});
            try std.testing.expectEqual(.complete, std.meta.activeTag(resumed.result));
            try resumed.result.complete.verify(audit_key.public_key);
            // Drop every current source lease before the real owner closes.
            // The signed checkpoint is durable; the resident cache is not.
            control.releaseForApply();
            try std.testing.expectEqual(@as(usize, 1), source.retireTable("items"));
            try control.reacquireAfterApply();
            const cold_state = try source.schemaOwnerStateAtRoot(7501, "items", .{});
            try std.testing.expectEqualDeep(recovered_state, cold_state);
            const cold_again = try source.stepSchemaOwnerAuditAtRoot(7501, "items", actual_binding, "", audit_key, .{}, .{});
            try std.testing.expect(cold_again.result == .needs_inputs);
            var recovered_audit = false;
            for (0..64) |iteration| {
                const turn = try source.stepSchemaOwnerAuditAtRoot(7501, "items", actual_binding, if (iteration == 0) inputs else "", audit_key, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{});
                try std.testing.expect(turn.result != .needs_inputs);
                if (turn.result == .complete) {
                    try turn.result.complete.verify(audit_key.public_key);
                    try std.testing.expectEqualDeep(resumed.result.complete.value.rows, turn.result.complete.value.rows);
                    recovered_audit = true;
                    break;
                }
            }
            try std.testing.expect(recovered_audit);
        }
        {
            // Real retained kernel observation plus a runtime barrier fixture:
            // a completed no-op does not rewrite the durable data entry (1,1).
            const attestation = @import("../metadata/schema_validation_attestation.zig");
            const Barrier = struct {
                captures: usize = 0,
                validations: usize = 0,
                fail: bool = false,
                reject_frozen: bool = false,
                certifications: usize = 0,
                expected_minimum: u64 = 2,
                fn wait(_: *anyopaque, _: u64, _: []const u8) !void {}
                fn capture(ptr: *anyopaque, group: u64) !read_gate.ReadSafetyBarrier.FrozenProof {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(@as(u64, 7501), group);
                    self.captures += 1;
                    return .{ .incarnation = 91, .term = 2, .applied_index = 2 };
                }
                fn validate(ptr: *anyopaque, group: u64, proof: read_gate.ReadSafetyBarrier.FrozenProof, _: ?u64, _: local.api_operation.CancellationToken) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(@as(u64, 7501), group);
                    try std.testing.expectEqualDeep(read_gate.ReadSafetyBarrier.FrozenProof{ .incarnation = 91, .term = 2, .applied_index = 2 }, proof);
                    self.validations += 1;
                    if (self.fail) return error.NotLeader;
                    if (self.reject_frozen) return error.ReadUnavailable;
                }
                fn certify(ptr: *anyopaque, group: u64, term: u64, minimum: u64, _: ?u64, _: local.api_operation.CancellationToken) !read_gate.ReadSafetyBarrier.FrozenProof {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try std.testing.expectEqual(@as(u64, 7501), group);
                    try std.testing.expectEqual(@as(u64, 2), term);
                    try std.testing.expectEqual(self.expected_minimum, minimum);
                    self.certifications += 1;
                    if (self.fail) return error.NotLeader;
                    return .{ .incarnation = 91, .term = 2, .applied_index = 3 };
                }
            };
            var barrier: Barrier = .{};
            const previous = source.read_safety_barrier;
            defer source.read_safety_barrier = previous;
            source.read_safety_barrier = .{ .ptr = &barrier, .vtable = &.{ .wait_read_safe = Barrier.wait, .capture_frozen = Barrier.capture, .validate_frozen = Barrier.validate } };
            const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(9));
            const root_key: local.storage_db_root_signing_identity.State = .{ .seed = @splat(9), .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
            const scope: attestation.RaftScope = .{
                .metadata_incarnation = "61616161616161616161616161616161".*,
                .binding_digest = try attestation.bindingDigest(actual_binding),
                .identity = .{ .metadata_incarnation = "61616161616161616161616161616161".*, .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root_key.public_key },
                .reporter_incarnation = 4,
                .replica_id = 5,
                .group_id = 7501,
                .root_generation = applied.root_generation,
                .physical_root_incarnation = applied.observation.physical_root_incarnation,
                .reported_term = 2,
                .reported_applied_index = 2,
            };
            const signed = try control.attest(scope, root_key);
            try signed.verify(scope);
            try std.testing.expectEqualDeep(applied.observation.applied, signed.rooted.observation.applied);
            try std.testing.expectEqual(@as(u64, 2), signed.rooted.completed.?.applied_index);
            try std.testing.expectEqual(@as(usize, 1), barrier.captures);
            try std.testing.expectEqual(@as(usize, 1), barrier.validations);
            var actual_data_scope = scope;
            actual_data_scope.reported_applied_index = 1;
            const fast = try control.attest(actual_data_scope, root_key);
            try fast.verify(actual_data_scope);
            try std.testing.expect(fast.rooted.completed == null);
            try std.testing.expectError(error.ReadSafetyBarrierUnavailable, control.attestCompleted(actual_data_scope, root_key));
            try std.testing.expectEqual(@as(usize, 1), barrier.captures);
            try std.testing.expectEqual(@as(usize, 1), barrier.validations);
            barrier.fail = true;
            {
                // Adapter qualification, not a native-stream/SQL activation
                // claim: a real checkpoint owns an opaque fixture payload and
                // the actual compiled owner's immutable header/root identity.
                // Native writer/receiver byte proofs have their own archive gate.
                barrier.fail = false;
                defer barrier.fail = true;
                const payloads = @import("../raft/storage/snapshot_payload_store.zig");
                const retention = @import("../raft/storage/snapshot_retention_source.zig");
                const wire = local.storage_db_native_raft_snapshot_contract;
                const storage = @import("../raft/storage/mod.zig");
                const parent_root = try std.fs.path.join(alloc, &.{ schema_root, "retained-parent" });
                defer alloc.free(parent_root);
                var layout = try storage.ReplicaPathLayout.initForReplica(alloc, parent_root, 7501, 5);
                defer layout.deinit(alloc);
                var persisted = try storage.PersistentReplicaState.init(alloc, layout);
                defer persisted.deinit();
                try persisted.groupStorage().persistReady(7501, .{
                    .entries = &.{.{ .index = 1, .term = 1, .data = @constCast("entry") }},
                    .hard_state = .{ .current_term = 1, .commit_index = 1 },
                });
                const header_bytes = try applied.observation.header.?.encode(alloc);
                defer alloc.free(header_bytes);
                const witness: wire.MaterializationWitness = .{
                    .native_offset = 0,
                    .native_size = 128,
                    .stream = .{
                        .identity = .{ .group_id = 7501, .namespace = local.storage_db_online_source_contract.namespaceBytes(actual_binding.fence.namespace), .through_index = 1, .native_term = 1, .native_index = 1, .origin = .{ .physical_root_incarnation = applied.observation.physical_root_incarnation, .namespace = actual_binding.fence.namespace, .authority = .{ .kind = .raft, .namespace = local.storage_db_online_source_contract.namespaceBytes(actual_binding.fence.namespace) }, .applied = .{ .term = applied.observation.applied.?.term, .index = applied.observation.applied.?.index }, .schema_header = local.storage_db_relational_schema_owner_contract.hash(header_bytes), .transaction_directory = null, .schema_outcomes = null }, .schema_header = applied.observation.header },
                        .footer_digest = @splat(31),
                    },
                };
                const manifest = try std.json.Stringify.valueAlloc(alloc, witness, .{});
                defer alloc.free(manifest);
                const spool = try std.fs.path.join(alloc, &.{ parent_root, "spool" });
                defer alloc.free(spool);
                const fixture_bytes: [128]u8 = @splat('a');
                try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = spool, .data = &fixture_bytes });
                const artifact = try @import("../raft/storage/file_snapshot_artifact.zig").FileSnapshotArtifact.createWithApplicationManifest(alloc, std.testing.io, spool, fixture_bytes.len, manifest);
                defer artifact.deinit();
                try persisted.groupStorage().compactSnapshotArtifact(alloc, 7501, .{ .index = 1, .term = 1 }, artifact, 1);
                const Retainer = struct {
                    state: *storage.PersistentReplicaState,
                    tamper: bool = false,
                    legacy: bool = false,
                    calls: usize = 0,
                    reopens: usize = 0,
                    fn reopen(ptr: *anyopaque, a: std.mem.Allocator, request: retention.Request, _: local.api_operation.RequestContext) !retention.RetainedReader {
                        const self: *@This() = @ptrCast(@alignCast(ptr));
                        self.reopens += 1;
                        return payloads.openRetained(a, std.testing.io, self.state.layout.snapshot_dir, request.reference, .{ .index = request.index, .term = request.term });
                    }
                    fn retain(ptr: *anyopaque, a: std.mem.Allocator, request: retention.Request, _: local.api_operation.RequestContext) !retention.RetainedSnapshot {
                        const self: *@This() = @ptrCast(@alignCast(ptr));
                        self.calls += 1;
                        var result = try self.state.retainPublishedSnapshotAlloc(a, .{ .index = request.index, .term = request.term }, request.reference);
                        errdefer result.deinit(a);
                        if (self.legacy) {
                            a.free(result.application_manifest);
                            result.application_manifest = &.{};
                            return result;
                        }
                        if (self.tamper) {
                            var arena = std.heap.ArenaAllocator.init(a);
                            defer arena.deinit();
                            var changed = try std.json.parseFromSliceLeaky(wire.MaterializationWitness, arena.allocator(), result.application_manifest, .{});
                            changed.stream.identity.origin.?.physical_root_incarnation += 1;
                            const changed_manifest = try std.json.Stringify.valueAlloc(a, changed, .{});
                            a.free(result.application_manifest);
                            result.application_manifest = changed_manifest;
                        }
                        return result;
                    }
                };
                var retainer: Retainer = .{ .state = &persisted };
                const request: retention.Request = .{ .group_id = 7501, .index = 1, .term = 1, .reference = @splat(23) };
                try std.testing.expectError(error.NativeSnapshotRequired, control.attestSnapshotParent(alloc, request, scope, root_key));
                source.schema_snapshot_retention = .{ .ptr = &retainer, .retain = Retainer.retain };
                defer source.schema_snapshot_retention = null;
                try std.testing.expectError(error.ReadSafetyBarrierUnavailable, control.attestSnapshotParent(alloc, request, scope, root_key));
                source.read_safety_barrier.vtable = &.{ .wait_read_safe = Barrier.wait, .capture_frozen = Barrier.capture, .validate_frozen = Barrier.validate, .certify_completed_prefix = Barrier.certify };
                // The query freeze must still reject a later quorum boundary;
                // immutable-image authorization certifies actual completion 3.
                barrier.reject_frozen = true;
                defer barrier.reject_frozen = false;
                try std.testing.expectError(error.ReadUnavailable, control.attest(scope, root_key));
                var parent = try control.attestSnapshotParent(alloc, request, scope, root_key);
                defer parent.deinit(alloc);
                defer payloads.releaseRetained(alloc, std.testing.io, layout.snapshot_dir, parent.retained) catch {};
                try parent.signed.verifyFor(actual_binding, applied.observation.physical_root_incarnation);
                try std.testing.expectEqualDeep(applied.observation.header.?, parent.source_header);
                try std.testing.expectEqual(@as(u64, 1), parent.signed.value.through_index);
                try std.testing.expectEqual(@as(u64, 3), parent.signed.value.completed.applied_index);
                try std.testing.expectEqual(@as(usize, 1), barrier.certifications);
                barrier.expected_minimum = 1;
                const replayed = try control.attestCompleted(actual_data_scope, root_key);
                try replayed.verify(actual_data_scope);
                try std.testing.expectEqual(@as(u64, 3), replayed.rooted.completed.?.applied_index);
                try std.testing.expectEqualDeep(fast.rooted.observation, replayed.rooted.observation);
                barrier.expected_minimum = 2;
                try std.testing.expectEqualDeep(witness.stream.footer_digest, parent.signed.value.stream_digest);
                // Recovery is an explicit capability, never a fallback which
                // silently creates another retention obligation.
                try std.testing.expectError(error.NativeSnapshotRequired, control.recoverSnapshotParent(alloc, request, scope, root_key));
                source.schema_snapshot_retention.?.reopen = Retainer.reopen;
                control.releaseForApply();
                try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, control.attestSnapshotParent(alloc, request, scope, root_key));
                try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, control.recoverSnapshotParent(alloc, request, scope, root_key));
                try control.reacquireAfterApply();
                var changed = request;
                changed.term = 3;
                try std.testing.expectError(error.InvalidControlReceiptPosition, control.attestSnapshotParent(alloc, changed, scope, root_key));
                changed = request;
                changed.index = 3;
                try std.testing.expectError(error.InvalidControlReceiptPosition, control.attestSnapshotParent(alloc, changed, scope, root_key));
                try std.testing.expectEqual(@as(usize, 1), retainer.calls);
                changed.index = 2;
                try std.testing.expectError(error.SnapshotNotPublished, control.attestSnapshotParent(alloc, changed, scope, root_key));
                retainer.tamper = true;
                try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, control.attestSnapshotParent(alloc, request, scope, root_key));
                retainer.tamper = false;
                retainer.legacy = true;
                try std.testing.expectError(error.NativeSnapshotRequired, control.attestSnapshotParent(alloc, request, scope, root_key));
                retainer.legacy = false;
                barrier.fail = true;
                try std.testing.expectError(error.NotLeader, control.attestSnapshotParent(alloc, request, scope, root_key));
                // Persist the real capture BEFORE certification. Leadership
                // loss there leaves a recoverable obligation, not a lost pin.
                const outbox = @import("../metadata/schema_validation_source_outbox.zig");
                const producer_root = try local.storage_db_root_signing_identity.loadOrCreate(alloc, std.testing.io, schema_root);
                var producer_scope = scope;
                producer_scope.identity.root_incarnation = producer_root.root_incarnation;
                producer_scope.identity.public_key = producer_root.public_key;
                var producer_request = request;
                producer_request.reference = @splat(27);
                producer_request.metadata_digest = try @import("../raft/storage/snapshot_metadata.zig").digest(producer_request.group_id, .{ .index = producer_request.index, .term = producer_request.term });
                const intent: outbox.Intent = .{
                    .lookup = .{ .group = 21, .table = actual_binding.fence.namespace.table_id, .id = actual_binding.id, .metadata_incarnation = producer_scope.metadata_incarnation, .plan_digest = actual_binding.plan_digest },
                    .ordinal = 0,
                    .revision = 1,
                    .root_epoch = 0,
                    .owner_digest = @splat(28),
                    .binding = actual_binding,
                    .scope = producer_scope,
                    .snapshot = .{ .index = producer_request.index, .term = producer_request.term },
                    .image = producer_request,
                };
                try std.testing.expectError(error.NotLeader, outbox.captureParent(@TypeOf(control), &control, alloc, std.testing.io, schema_root, intent, producer_root, .{}));
                const namespace = local.storage_db_online_source_contract.namespaceBytes(actual_binding.fence.namespace);
                const capture_serial = serial: {
                    var slot = try outbox.open(alloc, std.testing.io, schema_root, producer_root.root_incarnation, namespace, .{});
                    defer slot.deinit();
                    try std.testing.expectEqual(outbox.Phase.captured, slot.record().?.phase);
                    break :serial slot.record().?.serial;
                };
                var recovered = try payloads.readRetained(alloc, std.testing.io, layout.snapshot_dir, request.reference, .{ .index = 1, .term = 1 });
                defer recovered.deinit(alloc);
                try std.testing.expectEqualStrings(manifest, recovered.application_manifest);
                try persisted.groupStorage().persistReady(7501, .{ .snapshot = .{ .metadata = .{ .index = 2, .term = 1 }, .data = @constCast("new-current") } });
                const pins = retainer.calls;
                barrier.fail = false;
                try std.testing.expectError(error.SnapshotNotPublished, control.attestSnapshotParent(alloc, request, scope, root_key));
                var old_parent = try control.recoverSnapshotParent(alloc, request, scope, root_key);
                defer old_parent.deinit(alloc);
                try old_parent.signed.verifyFor(actual_binding, applied.observation.physical_root_incarnation);
                try std.testing.expectEqualDeep(parent.signed, old_parent.signed);
                try std.testing.expectEqualDeep(parent.retained.digest, old_parent.retained.digest);
                try std.testing.expectEqualStrings(manifest, old_parent.retained.application_manifest);
                try std.testing.expectEqual(pins + 1, retainer.calls); // Only the explicit failed new pin.
                var produced = try outbox.captureParent(@TypeOf(control), &control, alloc, std.testing.io, schema_root, intent, producer_root, .{});
                defer produced.deinit(alloc);
                defer payloads.releaseRetained(alloc, std.testing.io, layout.snapshot_dir, produced.retained) catch {};
                try produced.signed.verifyFor(actual_binding, applied.observation.physical_root_incarnation);
                try std.testing.expectEqualDeep(parent.retained.digest, produced.retained.digest);
                {
                    var slot = try outbox.open(alloc, std.testing.io, schema_root, producer_root.root_incarnation, namespace, .{});
                    defer slot.deinit();
                    try std.testing.expectEqual(capture_serial, slot.record().?.serial);
                    try slot.prepare(.{ .lookup = intent.lookup, .ordinal = intent.ordinal, .revision = intent.revision, .root_epoch = intent.root_epoch, .owner_digest = intent.owner_digest, .signed = produced.signed, .source_header = produced.source_header }, "fixture-owned-prepared-grant");
                }
                const reopens = retainer.reopens;
                try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, outbox.captureParent(@TypeOf(control), &control, alloc, std.testing.io, schema_root, intent, producer_root, .{}));
                try std.testing.expectEqual(reopens, retainer.reopens);
                changed = request;
                changed.reference[0] ^= 1;
                try std.testing.expectError(error.FileNotFound, control.recoverSnapshotParent(alloc, changed, scope, root_key));
                changed = request;
                changed.term = 2;
                try std.testing.expectError(error.SnapshotPayloadIdentityMismatch, control.recoverSnapshotParent(alloc, changed, scope, root_key));
                barrier.fail = true;
                try std.testing.expectError(error.NotLeader, control.recoverSnapshotParent(alloc, request, scope, root_key));
                try std.testing.expectEqual(pins + 1, retainer.calls); // Recovery never repins CURRENT.
            }
            try std.testing.expectError(error.NotLeader, control.attest(scope, root_key));
            source.read_safety_barrier.vtable = &.{ .wait_read_safe = Barrier.wait, .capture_frozen = Barrier.capture };
            try std.testing.expectError(error.ReadSafetyBarrierUnavailable, control.attest(scope, root_key));
        }
        control.releaseForApply();
        try std.testing.expectEqual(@as(usize, 1), source.retireTable("items"));
        const retained_path = try std.fs.path.join(alloc, &.{ schema_root, "retained-original" });
        defer alloc.free(retained_path);
        const replacement_path = try std.fs.path.join(alloc, &.{ schema_root, "retained-replacement" });
        defer alloc.free(replacement_path);
        // Simulate a genuinely different disk at the SAME visible generation.
        // Preserve both roots in the temporary fixture; do not relabel either.
        try std.Io.Dir.renameAbsolute(seed_path, retained_path, std.testing.io);
        checkpoint = "same generation physical disk replacement";
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, control.reacquireAfterApply());
        // Observing a missing owner must not provision an empty replacement.
        // Explicit first-use provisioning creates this different physical disk.
        {
            var provisioned = try @import("kernel_owner_client.zig").Owner.open(.{ .path = .fromSlice(seed_path), .table_name = .fromSlice("items"), .group_id = 7501, .lsm_root_generation = table_reads.backend_current_root_generation, .has_identity_namespace = 1, .identity_table_id = 101, .identity_shard_id = 7501, .identity_range_id = 7501, .online_source_authority = 1, .schema_json = .fromSlice(before_json), .indexes_json = .fromSlice("{}") });
            defer provisioned.deinit();
            // Establish an actual committed replacement owner, not only an
            // opened descriptor whose lazy catalog has not been exercised.
            const row = try table_writes.encodeStorageKernelBatchRequest(alloc, .{ .timestamp_ns = 1, .writes = &.{.{ .key = "replacement", .value = "{\"id\":1}" }} });
            defer alloc.free(row);
            var committed = try provisioned.replicatedBatchAtRaftEntryJson("items", row, 1, 1);
            committed.deinit();
        }
        checkpoint = "observe committed replacement disk";
        const replacement = try source.raftSchemaOwnerObservationAtRoot(7501, "items", actual_binding, .{});
        try std.testing.expectEqual(applied.root_generation, replacement.root_generation);
        try std.testing.expect(applied.observation.physical_root_incarnation != replacement.observation.physical_root_incarnation);
        try std.testing.expect(replacement.observation.header == null and replacement.observation.applied == null);
        try std.testing.expectEqual(@as(usize, 1), source.retireTable("items"));
        try std.Io.Dir.renameAbsolute(seed_path, replacement_path, std.testing.io);
        try std.Io.Dir.renameAbsolute(retained_path, seed_path, std.testing.io);
        try control.reacquireAfterApply();
        try std.testing.expectEqualDeep(applied, try control.recoverCommitted());

        // Real kernel tickets pin the old physical owner even for a future
        // entry. The queue fixture supplies a trusted preparation directly:
        // protocol 26 remains reserved, so this is not raw ASRL admission.
        const QueueHost = struct {
            prepared: ?kernel_owner_source.ProvisionedKernelOwnerSource.PreparedRaftSchemaControl,
            pub fn prepare(self: *@This(), _: @import("../data/schema_apply_queue.zig").Key, _: *const std.atomic.Value(bool)) !kernel_owner_source.ProvisionedKernelOwnerSource.PreparedRaftSchemaControl {
                const value = self.prepared orelse return error.TestMissingPreparation;
                self.prepared = null;
                return value;
            }
            pub fn notify(_: *@This()) void {}
        };
        const freeze_bytes = try (local.storage_db_relational_schema_owner_contract.Command{ .binding = actual_binding, .action = .freeze }).encodeRaft(alloc);
        defer alloc.free(freeze_bytes);
        var queue_host: QueueHost = .{ .prepared = try control.prepareCommitted(freeze_bytes, 1, 2) };
        defer if (queue_host.prepared) |*value| value.deinit();
        control.releaseForApply();
        const future_entry: @import("../data/schema_apply_queue.zig").Key = .{ .group = 7501, .term = 1, .index = 2 };
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.schema_apply.applyOrEnqueue(future_entry));
        try std.testing.expectEqual(.worked, source.schema_apply.step(&queue_host, 0));
        // A root replacement at cut 1 must also drain the ticket for entry 2.
        source.discardSchemaApplyForRootReplacement(7501);
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.schema_apply.applyOrEnqueue(future_entry));
        try std.testing.expectEqual(.worked, source.schema_apply.step(&queue_host, 0));
        try std.testing.expectEqual(@as(usize, 1), source.retireTable("items"));
        try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
        try control.reacquireAfterApply();
        try std.testing.expectEqualDeep(applied, try control.recoverCommitted());
        // Same membership/key, independent preparation after cold reopen.
        // Discard was neither a durable acknowledgement nor retirement.
        queue_host.prepared = try control.prepareCommitted(freeze_bytes, 1, 2);
        control.releaseForApply();
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.schema_apply.applyOrEnqueue(future_entry));
        try std.testing.expectEqual(.worked, source.schema_apply.step(&queue_host, 0));
        try source.schema_apply.applyOrEnqueue(future_entry);
        try std.testing.expectEqual(.worked, source.schema_apply.step(&queue_host, 0));
        try control.reacquireAfterApply();
        const frozen = try control.recoverCommitted();
        try std.testing.expectEqual(.scanning, frozen.observation.header.?.phase);
        try std.testing.expectEqual(@as(u64, 2), frozen.observation.applied.?.index);
        try std.testing.expectEqual(applied.observation.physical_root_incarnation, frozen.observation.physical_root_incarnation);
        {
            const cancel_bytes = try (local.storage_db_relational_schema_owner_contract.Command{ .binding = actual_binding, .action = .cancel }).encodeRaft(alloc);
            defer alloc.free(cancel_bytes);
            var cancel_ticket = try control.prepareCommitted(cancel_bytes, 1, 3);
            defer cancel_ticket.deinit();
            try cancel_ticket.apply(.{});
        }
        {
            // The private ABI must preserve the distinction between a durable
            // rejection and a failed preparation. The job stays canceled,
            // while the exact real term/index advances and survives replay.
            var rejected_ticket = try control.prepareCommitted(freeze_bytes, 1, 4);
            defer rejected_ticket.deinit();
            try std.testing.expectError(error.SchemaValidationCommandRejected, rejected_ticket.apply(.{}));
            try std.testing.expectError(error.SchemaValidationCommandRejected, rejected_ticket.apply(.{}));
            const rejected_cut = try rejected_ticket.observeCommitted();
            try std.testing.expectEqual(.canceled, rejected_cut.observation.header.?.phase);
            try std.testing.expectEqual(@as(u64, 3), rejected_cut.observation.header.?.updated.raft.index);
            try std.testing.expectEqual(@as(u64, 4), rejected_cut.observation.applied.?.index);
        }
        control.releaseForApply();
        try std.testing.expectEqual(@as(usize, 1), source.retireTable("items"));
        try control.reacquireAfterApply();
        try std.testing.expectError(error.SchemaValidationCommandRejected, control.prepareCommitted(freeze_bytes, 1, 4));
        // Restore/root publication must invalidate the same real pinned
        // tickets, not depend on the data-Raft snapshot caller doing it first.
        queue_host.prepared = try control.prepareCommitted(freeze_bytes, 1, 2);
        control.releaseForApply();
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.schema_apply.applyOrEnqueue(future_entry));
        try std.testing.expectEqual(.worked, source.schema_apply.step(&queue_host, 0));
        try source.startApplyControl(std.testing.io);
        var publication = try source.snapshotSource().beginPublication(.{ .io = std.testing.io, .group_id = 7501, .table_name = "items" });
        defer publication.deinit();
        try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
        publication.deinit();
        try control.reacquireAfterApply();
        const after_publication = try control.recoverCommitted();
        try std.testing.expectEqual(.canceled, after_publication.observation.header.?.phase);
        try std.testing.expectEqual(@as(u64, 4), after_publication.observation.applied.?.index);
        checkpoint = "native snapshot schema control replay";
        try qualifySchemaSnapshotReplay(alloc, schema_root, catalog.source(), before_json, inputs, actual_binding);
    }
}

/// Actual native snapshot materialization/installation followed by retained
/// control replay. Committed entries are fixture inputs, not quorum evidence;
/// no metadata root adoption or source grant is inferred from applying them.
fn qualifySchemaSnapshotReplay(
    a: std.mem.Allocator,
    root: []const u8,
    catalog: table_catalog.CatalogSource,
    before: []const u8,
    inputs: []const u8,
    seed_binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding,
) !void {
    const client = @import("kernel_owner_client.zig");
    const raw_mod = @import("data_raft_apply_client.zig");
    const log = @import("../data/schema_control_log.zig");
    const machine = @import("../raft/state_machine/mod.zig");
    const group = seed_binding.fence.owner_group_id;
    const namespace = seed_binding.fence.namespace;
    const Generation = struct {
        value: u64,
        fn get(ptr: *anyopaque, _: u64) u64 {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).value;
        }
        fn source(self: *@This()) table_reads.GroupVisibleRootGenerationSource {
            return .{ .ptr = self, .visible_root_generation_for_group = get };
        }
    };
    var source_generation: Generation = .{ .value = 11 };
    var destination_generation: Generation = .{ .value = 7 };
    const source_root = try std.fs.path.join(a, &.{ root, "snapshot-replay-source" });
    defer a.free(source_root);
    const destination_root = try std.fs.path.join(a, &.{ root, "snapshot-replay-destination" });
    defer a.free(destination_root);
    const source_path = try std.fmt.allocPrint(a, "{s}/group-{d}/table-db", .{ source_root, group });
    defer a.free(source_path);
    const destination_path = try std.fmt.allocPrint(a, "{s}/group-{d}/table-db", .{ destination_root, group });
    defer a.free(destination_path);
    const source_projection = try std.fs.path.join(a, &.{ source_root, "projection" });
    defer a.free(source_projection);
    const destination_projection = try std.fs.path.join(a, &.{ destination_root, "projection" });
    defer a.free(destination_projection);
    const row: db_mod.types.BatchRequest = .{ .timestamp_ns = 17, .writes = &.{.{ .key = "snapshot-row", .value = "{\"id\":17}" }} };
    const row_json = try table_writes.encodeStorageKernelBatchRequest(a, row);
    defer a.free(row_json);
    const lookup = try table_reads.encodeStorageKernelLookupRequest(a, "snapshot-row", .{});
    defer a.free(lookup);
    var expected_row: ?[]u8 = null;
    defer if (expected_row) |value| a.free(value);
    var expected_version: u64 = 0;
    var expected_digest: ?[32]u8 = null;
    const binding = prepared_binding: {
        var seed = try client.Owner.open(.{ .path = .fromSlice(source_path), .table_name = .fromSlice("items"), .group_id = group, .lsm_root_generation = source_generation.value, .has_identity_namespace = 1, .identity_table_id = namespace.table_id, .identity_shard_id = namespace.shard_id, .identity_range_id = namespace.range_id, .online_source_authority = 1, .schema_json = .fromSlice(before), .indexes_json = .fromSlice("{}") });
        defer seed.deinit();
        var written = try seed.replicatedBatchAtRaftEntryJson("items", row_json, 1, 2);
        written.deinit();
        var found = try seed.lookupJson("items", lookup);
        defer found.deinit();
        expected_row = try a.dupe(u8, found.bytes());
        expected_version = found.version();
        expected_digest = found.expectedContentDigest();
        const identity_request = try table_reads.encodeStorageKernelLookupRequest(a, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" });
        defer a.free(identity_request);
        var identity_bytes = try seed.lookupJson("items", identity_request);
        defer identity_bytes.deinit();
        var topology = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity, a, identity_bytes.bytes(), .{});
        defer topology.deinit();
        var fence = seed_binding.fence;
        fence.admission_epoch = topology.value.next_epoch;
        fence.catalog_digest = topology.value.catalog_digest;
        const ready = try seed.schemaOwnerPreparation("items", .{ .id = seed_binding.id, .provisional_plan = seed_binding.plan_digest, .fence = fence, .authority = .raft }, inputs, .{});
        break :prepared_binding ready.binding;
    };
    var raw = try raw_mod.RaftApplyStore.init(a, .{ .root_dir = source_projection, .native_source_delegate = true });
    defer raw.deinit();
    var source = kernel_owner_source.ProvisionedKernelOwnerSource.init(a, source_root, catalog, read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    _ = source.withGroupVisibleRootGeneration(source_generation.source()).withSchemaEntryStore(&raw);
    try source.startApplyControl(std.testing.io);
    // Use a real ordered decoder barrier, followed by BEGIN and freeze at
    // their exact committed positions. The snapshot carries this same cut.
    const barrier = try @import("../data/raft_batch.zig").encodeProtocolBarrier(a, "items", log.protocol_version);
    defer a.free(barrier);
    const begin_bytes = try (@import("antfly_local_sources").storage_db_relational_schema_owner_contract.Command{ .binding = binding, .action = .begin, .payload = inputs }).encodeRaft(a);
    defer a.free(begin_bytes);
    const freeze_bytes = try (@import("antfly_local_sources").storage_db_relational_schema_owner_contract.Command{ .binding = binding, .action = .freeze }).encodeRaft(a);
    defer a.free(freeze_bytes);
    const begin_frame = try log.encode(a, "items", source_generation.value, begin_bytes);
    defer a.free(begin_frame);
    const freeze_frame = try log.encode(a, "items", source_generation.value, freeze_bytes);
    defer a.free(freeze_frame);
    const row_frame = try @import("../data/raft_batch.zig").encodeWithStorageOwnerDescriptor(a, "items", row, .{ .lsm_root_generation = source_generation.value, .identity = .{ .table_id = namespace.table_id, .shard_id = namespace.shard_id, .range_id = namespace.range_id }, .schema_json = before, .indexes_json = "{}" });
    defer a.free(row_frame);
    const entries = try machine.encodeCommittedEntries(a, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = barrier },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = row_frame },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = begin_frame },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = freeze_frame },
    });
    defer a.free(entries);
    try raw.applyBatch(group, 4, entries);
    try waitSchemaReplay(&source, group, 3);
    try waitSchemaReplay(&source, group, 4);
    const frozen = try source.raftSchemaOwnerObservationAtRoot(group, "items", binding, .{});
    try std.testing.expectEqual(.scanning, frozen.observation.header.?.phase);
    var prepared = (try raw.prepareSnapshot(group, 4)) orelse return error.TestExpectedSnapshot;
    defer prepared.deinit();
    var capture: client.NativeSnapshotCapture = .{ .handle = try source.captureNativeRaftSnapshot(group, 4) };
    defer capture.deinit();
    try prepared.attachNative(capture.handle.?);
    capture.handle = null;
    var image = try prepared.materializeFile(a);
    defer image.deinit(a);
    const encoded = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, image.path, a, .limited(image.size + 1));
    defer a.free(encoded);
    var target = try raw_mod.RaftApplyStore.init(a, .{ .root_dir = destination_projection, .native_source_delegate = true });
    defer target.deinit();
    const incoming: abi.SnapshotPrepareRequest = .{ .path = .fromSlice(destination_path), .table_name = .fromSlice("items"), .group_id = group, .lsm_root_generation = destination_generation.value, .identity_table_id = namespace.table_id, .identity_shard_id = namespace.shard_id, .identity_range_id = namespace.range_id, .schema_json = .fromSlice(before), .encoded_snapshot = .fromSlice(encoded), .projection_store = target.handle, .expected_applied_index = 4 };
    {
        // Abandon a real prepared import before publication. Reopening the
        // same exact stream must recover without leaving a live writer or
        // losing the source cut needed by the following committed tail.
        var interrupted = try client.Snapshot.prepare(incoming);
        interrupted.deinit();
    }
    {
        var staged = try client.Snapshot.prepare(incoming);
        defer staged.deinit();
        try staged.promote();
        _ = try staged.publishPrepared();
        try staged.commit();
    }
    const cancel_bytes = try (@import("antfly_local_sources").storage_db_relational_schema_owner_contract.Command{ .binding = binding, .action = .cancel }).encodeRaft(a);
    defer a.free(cancel_bytes);
    const cancel_frame = try log.encode(a, "items", source_generation.value, cancel_bytes);
    defer a.free(cancel_frame);
    const tail = try machine.encodeCommittedEntries(a, &.{.{ .term = 1, .index = 5, .entry_type = .normal, .data = cancel_frame }});
    defer a.free(tail);
    try target.applyBatch(group, 5, tail);
    var installed_root: u128 = 0;
    for (0..2) |restart| {
        var destination = kernel_owner_source.ProvisionedKernelOwnerSource.init(a, destination_root, catalog, read_gate.alreadyReadSafeBarrier());
        defer destination.deinit();
        _ = destination.withGroupVisibleRootGeneration(destination_generation.source()).withSchemaEntryStore(&target);
        try destination.startApplyControl(std.testing.io);
        const restored = try destination.raftSchemaOwnerObservationAtRoot(group, "items", binding, .{});
        try std.testing.expectEqual(@as(@TypeOf(restored.observation.header.?.phase), if (restart == 0) .scanning else .canceled), restored.observation.header.?.phase);
        try std.testing.expect(restored.observation.physical_root_incarnation != frozen.observation.physical_root_incarnation);
        try std.testing.expectEqual(destination_generation.value + 1, restored.root_generation);
        if (restart == 0) installed_root = restored.observation.physical_root_incarnation else try std.testing.expectEqual(installed_root, restored.observation.physical_root_incarnation);
        try waitSchemaReplay(&destination, group, 5);
        const canceled = try destination.raftSchemaOwnerObservationAtRoot(group, "items", binding, .{});
        try std.testing.expectEqual(.canceled, canceled.observation.header.?.phase);
        try std.testing.expectEqual(@as(u64, 5), canceled.observation.applied.?.index);
        try std.testing.expectEqual(installed_root, canceled.observation.physical_root_incarnation);
        var foreign = binding;
        foreign.fence.namespace.range_id += 1;
        try std.testing.expectError(error.IdentityNamespaceMismatch, destination.acquireRaftSchemaOwnerSession(group, "items", foreign, .{}));
        // Serving reopens the historical owner only after checking the real
        // catalog. No recovery task or metadata grant is needed to replay an
        // already-committed terminal entry, including after cold reopening.
        var found = (try destination.readSource().lookupGroupLocal(a, group, "items", "snapshot-row", .{}, .stale)) orelse return error.TestExpectedRow;
        defer found.deinit(a);
        try std.testing.expectEqualStrings(expected_row.?, found.json);
        try std.testing.expectEqual(expected_version, found.version);
        try std.testing.expectEqual(expected_digest, found.expected_content_digest);
    }
}

fn waitSchemaReplay(source: *kernel_owner_source.ProvisionedKernelOwnerSource, group: u64, index: u64) !void {
    const limit = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        source.applyPreparedSchemaEntry(group, 1, index) catch |err| {
            if (err != error.RaftApplyWriterUnavailable) return err;
            if (@import("antfly_platform").time.monotonicNs() >= limit) return error.TestSchemaControlDidNotComplete;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            continue;
        };
        return;
    }
}

test "native index owner adapter executes fresh permission stages and recovers reserved LSM receipts after restart" {
    const alloc = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const publication = @import("../metadata/relational_index_publication.zig");
    const execution = @import("../api/relational_index_owner_execution.zig");
    const progress = local.storage_db_relational_index_publication_progress;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    var plan = try publication.testing.plan(scratch);
    @constCast(plan.owners)[0].receipt_authority = .native;
    const Catalog = struct {
        tables: [1]metadata_table_manager.TableRecord,
        ranges: [1]metadata_table_manager.RangeRecord,
        metadata_incarnation: [32]u8,
        fn pointSnapshot(ptr: *anyopaque, table: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings(self.tables[0].name, table);
            // The fixture has exactly one table/range, including full schema.
            // Declare its bounded point capability; do not weaken the serving
            // guard against unbounded admin fallback under a deadline.
            return table_catalog.TestAdminRoutingAdapter(snapshot, free).routingSnapshot(ptr, deadline_ns);
        }
        fn snapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 13, .metadata_incarnation = self.metadata_incarnation, .metrics = .{} },
                .tables = &self.tables,
                .ranges = &self.ranges,
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn validate(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
            var current = try snapshot(ptr);
            return contract.matches(&current);
        }
        fn source(self: *@This()) table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{
                .admin_snapshot = snapshot,
                .free_admin_snapshot = free,
                .linearizable_routing_snapshot = table_catalog.TestAdminRoutingAdapter(snapshot, free).linearizableSnapshot,
                .free_routing_snapshot = table_catalog.TestAdminRoutingAdapter(snapshot, free).freeRoutingSnapshot,
                .validate_publication = validate,
                .table_routing_snapshot = pointSnapshot,
            } };
        }
    };
    const Metadata = struct {
        value: publication.OwnerDecision,
        admission_reads: usize = 0,
        acceptance: ?@import("../metadata/relational_index_delivery_journal.zig").Head = null,
        acceptance_pending: bool = false,
        acceptance_reads: usize = 0,
        acceptance_writes: usize = 0,
        lose_acceptance_reply: bool = false,
        completed: bool = false,
        completion_writes: usize = 0,
        lose_completion_reply: bool = true,
        cleanup_ack: ?publication.Ack = null,
        cleanup_acks: usize = 0,
        release_acks: usize = 0,
        cancellation_status_reads: usize = 0,
        lose_cleanup_ack_reply: bool = true,
        lose_release_ack_reply: bool = true,
        pub fn currentAuthority(self: *@This()) !?@import("../metadata/relational_index_worker.zig").Authority {
            const view = self.value.native_signing orelse return null;
            return .{ .metadata_incarnation = view.identity.metadata_incarnation, .binding_digest = view.binding_digest, .writer_generation = 1 };
        }
        pub fn workJson(self: *@This(), a: std.mem.Allocator, query: @import("../metadata/relational_index_work.zig").Query) ![]u8 {
            const work = @import("../metadata/relational_index_work.zig");
            const response: work.Response = switch (query) {
                .discover => |after| .{ .discovered = .{ .head = if (after == 0 and self.value.head.phase != .canceled) self.value.head else null, .after_table = self.value.head.table_id, .exhausted = after != 0 or self.value.head.phase == .canceled } },
                .head => |identity| blk: {
                    try identity.requireHead(self.value.head);
                    break :blk .{ .head = self.value.head };
                },
                .routes => |request_| blk: {
                    try request_.identity.requireHead(self.value.head);
                    if (request_.ordinal != 0 or self.value.head.owner_count != 1) return error.InvalidCatalogRecord;
                    break :blk .{ .routes = .{ .head = self.value.head, .first_ordinal = 0, .table_name = self.value.dispatch.?.table_name, .targets = .{ .items = &.{.{ .owner = self.value.owner.owner, .start = "", .end = "" }} }, .next_ordinal = null } };
                },
            };
            return std.json.Stringify.valueAlloc(a, response, .{});
        }
        pub fn systemCatalog(self: *@This(), a: std.mem.Allocator, context: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(context.index_publication_authority);
            if (call == .relational_index_owner_ack or call == .relational_index_owner_ack_status) {
                const ack = if (call == .relational_index_owner_ack) call.relational_index_owner_ack else call.relational_index_owner_ack_status;
                try ack.validate();
                if (call == .relational_index_owner_ack) {
                    const update = try progress.acknowledge(self.value.head, self.value.owner, ack.expected_revision, ack.signed.receipt);
                    self.value.head = update.head;
                    self.value.owner = update.owner;
                    self.value.owner.attestations[@backingInt(ack.signed.receipt.action)] = try ack.signed.commitment();
                    if (ack.signed.receipt.action == .canceled) {
                        self.cleanup_ack = ack;
                        self.cleanup_acks += @intFromBool(update.changed);
                        self.value.cancellation_release = .{ .canceled = ack.cancellation.?.record, .cleanup_proof = ack.signed.proof, .decision_digest = try publication.cancellationReleaseDecisionDigest(self.value.metadata_group_id, self.value.metadata_incarnation, self.value.head) };
                        if (self.lose_cleanup_ack_reply) {
                            self.lose_cleanup_ack_reply = false;
                            return error.MetadataMutationOutcomeUnknown;
                        }
                    } else {
                        try std.testing.expectEqual(progress.Action.cancel_released, ack.signed.receipt.action);
                        self.release_acks += @intFromBool(update.changed);
                        if (self.lose_release_ack_reply) {
                            self.lose_release_ack_reply = false;
                            return error.MetadataMutationOutcomeUnknown;
                        }
                    }
                } else self.cancellation_status_reads += 1;
                return std.json.Stringify.valueAlloc(a, publication.AckStatus{ .head = self.value.head, .owner = self.value.owner, .replication_pending = false }, .{});
            }
            if (call == .relational_index_source_complete) {
                const journal = @import("../metadata/relational_index_delivery_journal.zig");
                const request_ = call.relational_index_source_complete;
                try request_.validate();
                try request_.consumed.requireHead(self.acceptance orelse return error.InvalidPrivateIndexClaimReceipt);
                if (!self.completed) self.completion_writes += 1;
                self.completed = true;
                if (self.lose_completion_reply) {
                    self.lose_completion_reply = false;
                    return error.MetadataMutationOutcomeUnknown;
                }
                return std.json.Stringify.valueAlloc(a, journal.Status{ .query = request_.query, .head = self.acceptance, .completed = true }, .{});
            }
            if (call == .relational_index_source_accept) {
                const journal = @import("../metadata/relational_index_delivery_journal.zig");
                const request = call.relational_index_source_accept;
                self.acceptance = try @import("../api/relational_index_delivery_producer.zig").testing.acceptedHead(request, self.acceptance);
                self.acceptance_writes += 1;
                if (self.lose_acceptance_reply) {
                    self.lose_acceptance_reply = false;
                    return error.MetadataMutationOutcomeUnknown;
                }
                return std.json.Stringify.valueAlloc(a, journal.Status{ .query = journal.Query.from(request), .head = self.acceptance }, .{});
            }
            if (call == .relational_index_source_accept_status) {
                self.acceptance_reads += 1;
                return std.json.Stringify.valueAlloc(a, @import("../metadata/relational_index_delivery_journal.zig").Status{ .query = call.relational_index_source_accept_status, .head = self.acceptance, .replication_pending = self.acceptance_pending, .completed = self.completed }, .{});
            }
            var value = self.value;
            if (call.relational_index_owner_decision.operation == .cancel or call.relational_index_owner_decision.operation == .cancel_release) {
                value.disposition = try publication.ownerPermission(value.head, value.owner, call.relational_index_owner_decision.operation);
                if (value.disposition == .acknowledged) {
                    value.dispatch = null;
                    value.inputs = null;
                    value.native_signing = null;
                    value.cancellation_release = null;
                    return std.json.Stringify.valueAlloc(a, value, .{});
                }
            }
            if (call.relational_index_owner_decision.operation != .cancel_release) value.cancellation_release = null;
            if (call.relational_index_owner_decision.claim_groups) |groups| {
                if (groups.items.len != 0) return error.UnexpectedFixtureDestination;
                value.claim_routes = .{ .items = &.{} };
            }
            if (call.relational_index_owner_decision.projection == .permission)
                value.inputs = null
            else
                self.admission_reads += 1;
            return std.json.Stringify.valueAlloc(a, value, .{});
        }
    };
    const Transport = struct {
        owners: *kernel_owner_source.ProvisionedKernelOwnerSource,
        metadata: *Metadata,
        signing_root: local.storage_db_root_signing_identity.State,
        calls: usize = 0,
        fn localTransport(self: *@This()) @import("../api/relational_index_local_delivery.zig").Transport {
            return .{ .control_port = .{ .ptr = self, .execute_fn = controlDirect }, .claim_port = .{ .ptr = self, .execute_fn = claimsDirect } };
        }
        fn controlDirect(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, owner_group: u64, value: execution.Request, context: local.api_operation.RequestContext) !execution.Result {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try value.validate(owner_group);
            self.calls += 1;
            var result = try execution.step(a, self.owners, self.metadata, table, value, context);
            errdefer result.deinit();
            if (result.needsSigning()) try result.signNative(self.signing_root, 2, 3, 4);
            if (result == .canceling) {
                const encoded = try std.json.Stringify.valueAlloc(a, try result.response(), .{});
                defer a.free(encoded);
                try std.testing.expect(encoded.len <= 32 * 1024);
                var wire = try std.json.parseFromSlice(execution.ControlResponse, a, encoded, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
                defer wire.deinit();
                try wire.value.validate(value);
                var foreign = wire.value;
                foreign.canceling.request.group_id += 1;
                try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, foreign.validate(value));
                if (wire.value.canceling.signed_ack != null) {
                    foreign = wire.value;
                    foreign.canceling.root_generation += 1;
                    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, foreign.validate(value));
                }
            }
            return result;
        }
        fn claimsDirect(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, owner_group: u64, value: execution.ClaimRequest, context: local.api_operation.RequestContext) !execution.ClaimResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try value.validate(owner_group);
            self.calls += 1;
            var result = try execution.claimStep(a, self.owners, self.metadata, table, value, context);
            errdefer result.deinit();
            if (result.needsSigning()) try result.signNative(self.signing_root, 2, 3, 4);
            return result;
        }
        fn parsed(comptime T: type, a: std.mem.Allocator, value: T) !std.json.Parsed(T) {
            const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
            defer a.free(bytes);
            return std.json.parseFromSlice(T, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        }
        pub fn control(self: *@This(), a: std.mem.Allocator, table: []const u8, value: execution.Request, context: local.api_operation.RequestContext) !std.json.Parsed(execution.ControlResponse) {
            self.calls += 1;
            var result = try execution.step(a, self.owners, self.metadata, table, value, context);
            defer result.deinit();
            if (result.needsSigning()) try result.signNative(self.signing_root, 2, 3, 4);
            return parsed(execution.ControlResponse, a, try result.response());
        }
        pub fn claims(self: *@This(), a: std.mem.Allocator, table: []const u8, value: execution.ClaimRequest, context: local.api_operation.RequestContext) !std.json.Parsed(execution.ClaimResponse) {
            self.calls += 1;
            var result = try execution.claimStep(a, self.owners, self.metadata, table, value, context);
            defer result.deinit();
            if (result.needsSigning()) try result.signNative(self.signing_root, 2, 3, 4);
            return parsed(execution.ClaimResponse, a, result.response());
        }
    };
    var catalog: Catalog = .{ .tables = .{plan.before}, .ranges = .{plan.owners[0].range}, .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower) };
    const group = plan.owners[0].range.group_id;
    const RootGeneration = struct {
        fn read(ptr: *anyopaque, _: u64) u64 {
            return @as(*const u64, @ptrCast(@alignCast(ptr))).*;
        }
    };
    // Choose a non-default descriptor generation, then recover the actual
    // persisted cut from that root after all process-local owners are closed.
    const root_generation: u64 = 7;
    var root_selection = try local.storage_db_root_signing_identity.rootSelectionForAttestedGeneration(root_generation);
    const request: execution.Request = .{ .id = plan.id, .table_id = plan.before.table_id, .group_id = group, .operation = .fence };
    var prepared: ?publication.Prepared = null;
    defer if (prepared) |*value| value.deinit();
    var metadata: Metadata = undefined;
    var reserved_receipt: progress.Receipt = undefined;
    var fence_receipt: progress.Receipt = undefined;
    {
        var owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, catalog.source(), read_gate.alreadyReadSafeBarrier());
        owners.online_source_authority = .native;
        owners.group_visible_root_generation = .{ .ptr = &root_selection, .visible_root_generation_for_group = RootGeneration.read };
        defer owners.deinit();
        // Read the actual newly provisioned owner's epoch and catalog instead
        // of guessing them from the metadata fixture's synthetic defaults.
        var catalog_response = (try owners.readSource().lookupGroupLocal(alloc, group, plan.before.name, "", .{ .relational_integrity_catalog = true }, .stale)).?;
        defer catalog_response.deinit(alloc);
        const observed = try std.json.parseFromSliceLeaky(struct { catalog: []const u8, schema_version: u32, table_id: []const u8 }, scratch, catalog_response.json, .{ .allocate = .alloc_always });
        try std.testing.expectEqual(@as(u32, 1), observed.schema_version);
        var identity_response = (try owners.readSource().lookupGroupLocal(alloc, group, plan.before.name, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" }, .stale)).?;
        defer identity_response.deinit(alloc);
        const identity = try std.json.parseFromSliceLeaky(local.storage_db_relational_integrity_topology_contract.Identity, scratch, identity_response.json, .{});
        const predecessor = try scratch.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(observed.catalog));
        try std.base64.standard.Decoder.decode(predecessor, observed.catalog);
        var predecessor_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(predecessor, &predecessor_digest, .{});
        try std.testing.expectEqual(identity.catalog_digest, predecessor_digest);
        @constCast(plan.owners)[0].admission_epoch = identity.next_epoch;
        @constCast(plan.owners)[0].catalog_digest = identity.catalog_digest;
        plan.catalog_before_b64 = observed.catalog;
        prepared = try plan.prepare(alloc);
        metadata = .{ .value = .{
            .metadata_group_id = 13,
            .metadata_incarnation = catalog.metadata_incarnation,
            .head = prepared.?.head,
            .owner = prepared.?.owners[0],
            .disposition = .execute,
            .dispatch = try std.json.parseFromSliceLeaky(publication.Dispatch, scratch, prepared.?.dispatch[0], .{ .allocate = .alloc_always }),
            .inputs = try std.json.parseFromSliceLeaky(publication.Inputs, scratch, prepared.?.inputs, .{ .allocate = .alloc_always }),
        } };
        var admitted = try execution.step(alloc, &owners, &metadata, plan.before.name, request, .{});
        defer admitted.deinit();
        try std.testing.expect(admitted == .applied);
        try std.testing.expect(admitted.rootGeneration() == null);
        try std.testing.expect(admitted.outcome().?.receipt == null);
        try std.testing.expect(!admitted.outcome().?.replication_pending);
        var fenced = try execution.step(alloc, &owners, &metadata, plan.before.name, request, .{});
        defer fenced.deinit();
        try std.testing.expect(fenced == .controlled);
        try std.testing.expectEqual(root_generation, fenced.rootGeneration().?);
        fence_receipt = fenced.outcome().?.receipt.?;
        try std.testing.expectEqual(.fenced, fence_receipt.action);
        try std.testing.expect(fence_receipt.position == .native);
        var recovered_fence = try execution.step(alloc, &owners, &metadata, plan.before.name, request, .{});
        defer recovered_fence.deinit();
        try std.testing.expect(recovered_fence == .recovered);
        try std.testing.expectEqual(root_generation, recovered_fence.recovered.root_generation);
        try std.testing.expectEqual(try fence_receipt.digest(), try recovered_fence.outcome().?.receipt.?.digest());
        // This metadata driver simulates permission progression only. Root
        // signature/enrollment verification is not bypassed in production.
        const next = try progress.acknowledge(metadata.value.head, metadata.value.owner, metadata.value.head.revision, fence_receipt);
        metadata.value.head = next.head;
        metadata.value.owner = next.owner;
        var reserve_request = request;
        reserve_request.operation = .reserve;
        var reserved = try execution.step(alloc, &owners, &metadata, plan.before.name, reserve_request, .{});
        defer reserved.deinit();
        try std.testing.expect(reserved == .controlled);
        try std.testing.expectEqual(root_generation, reserved.rootGeneration().?);
        reserved_receipt = reserved.outcome().?.receipt.?;
        try std.testing.expectEqual(.reserved, reserved_receipt.action);
        try std.testing.expect(reserved_receipt.position.native.sequence > fence_receipt.position.native.sequence);
        var public_schema = (try owners.readSource().lookupGroupLocal(alloc, group, plan.before.name, "", .{ .relational_topology_json = "{\"mode\":\"public_schema\"}" }, .stale)).?;
        defer public_schema.deinit(alloc);
        const schema_json = try std.json.parseFromSliceLeaky([]const u8, scratch, public_schema.json, .{ .allocate = .alloc_always });
        const schema_value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, schema_json, .{});
        try std.testing.expectEqual(@as(i64, 1), schema_value.object.get("version").?.integer);
    }
    {
        var owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, catalog.source(), read_gate.alreadyReadSafeBarrier());
        owners.online_source_authority = .native;
        owners.group_visible_root_generation = .{ .ptr = &root_selection, .visible_root_generation_for_group = RootGeneration.read };
        defer owners.deinit();
        var reserve_request = request;
        reserve_request.operation = .reserve;
        var recovered = try execution.step(alloc, &owners, &metadata, plan.before.name, reserve_request, .{});
        defer recovered.deinit();
        try std.testing.expect(recovered == .recovered);
        try std.testing.expectEqual(root_generation, recovered.recovered.root_generation);
        try std.testing.expect(recovered.outcome().?.duplicate);
        try std.testing.expectEqual(try reserved_receipt.digest(), try recovered.outcome().?.receipt.?.digest());
        try std.testing.expectEqual(reserved_receipt.position.native.sequence, recovered.outcome().?.position.sequence);
        var observed = (try owners.writeSource().localRuntimeStatusGroupLocal(alloc, group, plan.before.name)) orelse return error.MissingOwnerObservation;
        defer observed.deinit(alloc);
        const authority = observed.metadata.source_authority orelse return error.MissingOwnerObservation;
        try std.testing.expectEqual(.native, authority.kind);
        try std.testing.expectEqualSlices(u8, &local.storage_db_online_source_contract.namespaceBytes(reserved_receipt.owner.namespace), &authority.namespace);
        try std.testing.expect(authority.sequence >= reserved_receipt.position.native.sequence);
        try std.testing.expectEqual(root_selection, observed.metadata.lsm_root_generation);
        try std.testing.expectEqual(@as(usize, 1), metadata.admission_reads);
        // Exercise cancellation through the actual leased compiled owner, not
        // a synthetic source callback. Metadata/RPC authentication is a
        // separate gate; these operations prove the private storage boundary.
        const native = local.storage_db_relational_index_owner_control;
        var canceled_binding = recovered.outcome().?.binding;
        var claim_binding = canceled_binding;
        claim_binding.phase = .reserved;
        const claim_command: native.ClaimCommand = .{
            .binding = claim_binding,
            .claims = &.{.{ .tuple = "leased\x00\xff\x80-claim", .parent_table = plan.before.name, .parent_key = "leased-owner", .schema_version = claim_binding.physical_candidate.schema_version }},
        };
        var rooted_claim = try owners.applyNativeIndexOwnerClaimsAtRoot(alloc, group, plan.before.name, claim_command, .{});
        defer rooted_claim.deinit();
        try std.testing.expectEqual(root_generation, rooted_claim.root_generation);
        const claimed = rooted_claim.outcome.value;
        try std.testing.expect(!claimed.duplicate and claimed.claims_added == 1);
        try std.testing.expectEqual(reserved_receipt.position.native.sequence + 1, claimed.position.sequence);
        var duplicate_claim = try owners.applyNativeIndexOwnerClaims(alloc, group, plan.before.name, claim_command, .{});
        defer duplicate_claim.deinit();
        try std.testing.expect(duplicate_claim.value.duplicate);
        try std.testing.expectEqual(claimed.position, duplicate_claim.value.position);
        var rooted_duplicate = try owners.applyNativeIndexOwnerClaimsAtRoot(alloc, group, plan.before.name, claim_command, .{});
        defer rooted_duplicate.deinit();
        try std.testing.expectEqual(rooted_claim.root_generation, rooted_duplicate.root_generation);
        try std.testing.expect(rooted_duplicate.outcome.value.duplicate);
        try std.testing.expectEqual(claimed.position, rooted_duplicate.outcome.value.position);
        // Advance the independently read metadata permission to build, then
        // exercise the receiver against this actual compiled reserved owner.
        const build_permission = try progress.acknowledge(metadata.value.head, metadata.value.owner, metadata.value.head.revision, reserved_receipt);
        metadata.value.head = build_permission.head;
        metadata.value.owner = build_permission.owner;
        const source_request: execution.Request = .{ .id = plan.id, .table_id = plan.before.table_id, .group_id = group, .operation = .source };
        {
            var absent = try execution.step(alloc, &owners, &metadata, plan.before.name, source_request, .{});
            defer absent.deinit();
            try std.testing.expect(absent == .source and absent.source.outcome.batch == null);
            try std.testing.expect(!absent.needsSigning());
        }
        // Test startup establishes the root key before reading/signing. Serving
        // code only loads the existing key and never creates one after a read.
        const signing_root = try local.storage_db_root_signing_identity.loadOrCreate(alloc, std.testing.io, root);
        const native_root: @import("../metadata/standalone_native_owner.zig").Binding = .{ .metadata_incarnation = metadata.value.metadata_incarnation, .node_id = 2, .store_id = 3, .root_incarnation = signing_root.root_incarnation };
        metadata.value.native_signing = .{ .identity = .{ .metadata_incarnation = native_root.metadata_incarnation, .node_id = 2, .store_id = 3, .root_incarnation = signing_root.root_incarnation, .public_key = signing_root.public_key }, .reporter_incarnation = 4, .binding_digest = try native_root.digest() };
        // ACK-status recovery authenticates every visited stage, not only
        // cancellation. Retain genuine signatures for the earlier native cuts.
        for ([_]progress.Receipt{ fence_receipt, reserved_receipt }) |receipt| {
            const view = metadata.value.native_signing.?;
            const proof = try (publication.RootProof{ .identity = view.identity, .reporter_incarnation = view.reporter_incarnation, .authority = .{ .native = view.binding_digest }, .root_generation = root_generation, .signature = undefined }).sign(signing_root, try receipt.digest());
            metadata.value.owner.attestations[@backingInt(receipt.action)] = try (publication.SignedReceipt{ .receipt = receipt, .proof = proof }).commitment();
        }
        var build_request = source_request;
        build_request.operation = .build;
        var built = try execution.step(alloc, &owners, &metadata, plan.before.name, build_request, .{});
        defer built.deinit();
        try std.testing.expect(built == .building and built.building.outcome.value == .advanced);
        var source_result = try execution.step(alloc, &owners, &metadata, plan.before.name, source_request, .{});
        defer source_result.deinit();
        try std.testing.expect(source_result.needsSigning());
        try source_result.signNative(signing_root, 2, 3, 4);
        const source_response = (try source_result.response()).source;
        var source_read = try source_response.decodeSigned(alloc, source_request);
        defer source_read.deinit();
        try std.testing.expectEqual(root_generation, source_response.signed_source.?.root_generation);
        try std.testing.expectEqual(@as(usize, 0), source_read.batch.?.rows().len);
        try std.testing.expectEqual(built.building.outcome.value.advanced.position, try source_read.batch.?.header.position());
        const closure_request: execution.ClaimRequest = .{ .id = plan.id, .table_id = plan.before.table_id, .group_id = group, .closure = .{ .ordinal = 1, .batch_digest = source_read.batch.?.header.digest(), .targets = .{ .items = &.{} } } };
        var complete_source = try execution.claimStep(alloc, &owners, &metadata, plan.before.name, closure_request, .{});
        defer complete_source.deinit();
        try std.testing.expect(complete_source == .closed);
        try complete_source.signNative(signing_root, 2, 3, 4);
        try complete_source.response().validate(closure_request);
        try std.testing.expectEqual(@as(usize, 0), complete_source.closed.owned.data.parts.items.len);
        try std.testing.expectEqual(root_generation, complete_source.closed.proof.?.root_generation);
        const delivery: execution.ClaimRequest = .{ .id = plan.id, .table_id = plan.before.table_id, .group_id = group, .claims = claim_command.claims };
        var received = try execution.claimStep(alloc, &owners, &metadata, plan.before.name, delivery, .{});
        defer received.deinit();
        try std.testing.expect(received == .applied);
        try std.testing.expectEqual(rooted_claim.root_generation, received.applied.root_generation);
        try std.testing.expect(received.applied.outcome.value.duplicate);
        try std.testing.expectEqual(claimed.position, received.applied.outcome.value.position);
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, owners.applyNativeIndexOwnerClaims(alloc, group + 1, plan.before.name, claim_command, .{}));
        // Synthetic metadata decision, actual source bytes and compiled apply.
        // This qualifies the private ABI, not independent metadata acceptance.
        const Source = local.storage_db_relational_index_owner_job.private_claims.SourceStream;
        const header = source_read.batch.?.header;
        var binding_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(&try source_read.binding.encode(), &binding_digest, .{});
        var accepted: Source.Accepted = .{
            .metadata_incarnation = metadata.value.metadata_incarnation,
            .binding_digest = binding_digest,
            .scope_digest = header.bytes[40..72].*,
            .ordinal = 1,
            .rows = 0,
            .batch_rows = 0,
            .source_sequence = (try header.position()).sequence,
            .batch_digest = header.digest(),
            .logical_digest = try complete_source.closed.owned.data.logicalDigest(),
            .previous_decision = @splat(0),
            .decision_digest = @splat(0),
            .state = @fromBackingInt(@intCast(header.bytes[184])),
        };
        accepted.decision_digest = accepted.computedDigest();
        const consume_command: native.ConsumeCommand = .{ .binding = source_read.binding, .accepted = accepted };
        const consume_request: execution.ClaimRequest = .{ .table_id = plan.before.table_id, .group_id = group, .id = plan.id, .consume = .{ .ordinal = 1, .batch_digest = header.digest() } };
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, execution.claimStep(alloc, &owners, &metadata, plan.before.name, consume_request, .{}));
        var accepted_head: @import("../metadata/relational_index_delivery_journal.zig").Head = .{
            .metadata_incarnation = accepted.metadata_incarnation,
            .binding_digest = accepted.binding_digest,
            .scope_digest = accepted.scope_digest,
            .ordinal = accepted.ordinal,
            .rows = accepted.rows,
            .batch_rows = accepted.batch_rows,
            .source_sequence = accepted.source_sequence,
            .batch_digest = accepted.batch_digest,
            .logical_digest = accepted.logical_digest,
            .previous_decision = accepted.previous_decision,
            .decision_digest = accepted.decision_digest,
            .accepted_root = @splat(8),
            .provenance = @splat(0),
            .previous_rows = 0,
            .previous_provenance = @splat(0),
            .state = accepted.state,
        };
        var provenance = std.crypto.hash.Blake3.init(.{});
        provenance.update("antfly/private-unique-accepted-provenance/v1");
        provenance.update(&accepted_head.previous_provenance);
        provenance.update(&accepted_head.decision_digest);
        provenance.update(&accepted_head.accepted_root);
        provenance.final(&accepted_head.provenance);
        metadata.acceptance = accepted_head;
        metadata.acceptance_pending = true;
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, execution.claimStep(alloc, &owners, &metadata, plan.before.name, consume_request, .{}));
        metadata.acceptance_pending = false;
        var wrong_batch = consume_request;
        wrong_batch.consume.?.batch_digest[0] ^= 1;
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, execution.claimStep(alloc, &owners, &metadata, plan.before.name, wrong_batch, .{}));
        // Drive the actual compiled owner, with explicitly synthetic metadata
        // acceptance. Lose the committed metadata reply, destroy all volatile
        // producer state, and resume without reissuing delivery/acceptance.
        const producer_module = @import("../api/relational_index_delivery_producer.zig");
        var cohort = try local.storage_db_relational_index_owner_job.private_claims.delivery.Cohort.init(alloc, &.{.{ .owner = metadata.value.owner.owner, .start = "", .end = "" }});
        defer cohort.deinit();
        var transport: Transport = .{ .owners = &owners, .metadata = &metadata, .signing_root = signing_root };
        var local_transport = transport.localTransport();
        const producer_context: local.api_operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 60 * std.time.ns_per_s };
        metadata.acceptance = null;
        metadata.lose_acceptance_reply = true;
        {
            var producer = try execution.DeliveryProducer.init(alloc, &cohort, plan.before.name, source_request, producer_context);
            defer producer.deinit();
            for (0..8) |_| {
                const before_calls = transport.calls;
                _ = try producer.step(&local_transport, &metadata);
                try std.testing.expect(transport.calls - before_calls <= 1);
                if (producer.phase == .resolve_accept) break;
            }
            try std.testing.expectEqual(producer_module.Phase.resolve_accept, producer.phase);
            try std.testing.expectEqual(@as(usize, 1), metadata.acceptance_writes);
            try std.testing.expect(producer.read == null and producer.plan == null and producer.delivered == null);
            try std.testing.expect(producer.closed != null);
            var retained = try owners.readNativeIndexClaimSourceAtRoot(alloc, group, plan.before.name, .{ .id = consume_command.binding.id, .plan_digest = consume_command.binding.plan_digest, .owner_digest = consume_command.binding.owner_digest, .ordinal = consume_command.binding.ordinal, .namespace = consume_command.binding.fence.namespace, .owner_group_id = group }, .{});
            defer retained.deinit();
            try std.testing.expect(retained.outcome.batch != null and retained.outcome.consumed == null);
        }
        var resumed = try execution.DeliveryProducer.init(alloc, &cohort, plan.before.name, source_request, producer_context);
        defer resumed.deinit();
        var producer_result: producer_module.Result = .{ .pending = .status };
        for (0..8) |_| {
            const before_calls = transport.calls;
            producer_result = try resumed.step(&local_transport, &metadata);
            try std.testing.expect(transport.calls - before_calls <= 1);
            if (producer_result == .advanced) break;
        }
        try std.testing.expect(producer_result == .advanced and producer_result.advanced.eof and producer_result.advanced.drained);
        try producer_result.advanced.consumed.verify();
        try std.testing.expectEqual(accepted.decision_digest, producer_result.advanced.consumed.decision_digest);
        try std.testing.expectEqual(@as(usize, 1), metadata.acceptance_writes);
        const finished_calls = transport.calls;
        _ = try resumed.step(&local_transport, &metadata);
        try std.testing.expectEqual(finished_calls, transport.calls);
        var consumed_result = try execution.claimStep(alloc, &owners, &metadata, plan.before.name, consume_request, .{});
        defer consumed_result.deinit();
        try std.testing.expect(consumed_result == .consumed and consumed_result.needsSigning());
        const consumed = consumed_result.consumed;
        try std.testing.expectEqual(root_generation, consumed.root_generation);
        // The resumed producer performed the first physical consumption.
        try std.testing.expect(consumed.outcome.value.duplicate and !consumed.outcome.value.replication_pending);
        try std.testing.expectEqualDeep(producer_result.advanced.consumed.position, @import("antfly_local_sources").storage_db_receipt_position.Position{ .native = consumed.outcome.value.position });
        try std.testing.expect(consumed.outcome.value.position.sequence > accepted.source_sequence);
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, consumed_result.response().requireSignedConsumption(consume_request));
        consumed_result.consumed.outcome.value.replication_pending = true;
        consumed_result.consumed.outcome.value.error_name = "ReplicationOutboxPending";
        try std.testing.expect(!consumed_result.needsSigning());
        try std.testing.expectError(error.NativeIndexSigningUnavailable, consumed_result.signNative(signing_root, 2, 3, 4));
        consumed_result.consumed.outcome.value.replication_pending = false;
        consumed_result.consumed.outcome.value.error_name = null;
        consumed_result.consumed.pinned_root_generation += 1;
        try std.testing.expectError(error.IndexOwnerReceiptChanged, consumed_result.signNative(signing_root, 2, 3, 4));
        consumed_result.consumed.pinned_root_generation -= 1;
        try consumed_result.signNative(signing_root, 2, 3, 4);
        const signed_consumption = try consumed_result.response().requireSignedConsumption(consume_request);
        try std.testing.expectEqual(accepted.decision_digest, signed_consumption.decision_digest);
        var forged_consumption = signed_consumption;
        forged_consumption.position.native.sequence += 1;
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, forged_consumption.verify());
        var wrong_authority = consumed.outcome.value.record.?;
        wrong_authority.position = .{ .raft = .{ .term = 1, .index = accepted.source_sequence + 1 } };
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, @import("../metadata/relational_index_delivery_journal.zig").Consumed.fromRecord(wrong_authority, signed_consumption.proof));
        metadata.acceptance = null;
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, execution.claimStep(alloc, &owners, &metadata, plan.before.name, consume_request, .{}));
        metadata.acceptance = accepted_head;
        var receive_retry = try execution.claimStep(alloc, &owners, &metadata, plan.before.name, consume_request, .{});
        defer receive_retry.deinit();
        try std.testing.expect(receive_retry.consumed.outcome.value.duplicate);
        try receive_retry.signNative(signing_root, 2, 3, 4);
        _ = try receive_retry.response().requireSignedConsumption(consume_request);
        try std.testing.expect(metadata.acceptance_reads >= 6);
        {
            // Synthetic work/metadata directory, actual leased compiled owner.
            // Automatic scheduling must resume a consumed prefix without
            // publishing an index or proposing acceptance a second time.
            var scheduler: @import("../metadata/relational_index_worker.zig").Coordinator = .{};
            defer scheduler.deinit(std.testing.io);
            const before = transport.calls;
            var now = @import("antfly_platform").time.monotonicNs();
            for (0..40) |_| {
                const calls_before = transport.calls;
                _ = try scheduler.step(std.testing.io, alloc, null, &metadata, &local_transport, now);
                try std.testing.expect(transport.calls - calls_before <= 1);
                if (transport.calls - before >= 2 and scheduler.jobs[0].?.owners[0] == null) break;
                now += 300 * std.time.ns_per_ms;
            }
            try std.testing.expectEqual(@as(usize, 2), transport.calls - before);
            try std.testing.expectEqual(@as(usize, 1), metadata.acceptance_writes);
            try std.testing.expectEqual(@as(usize, 1), metadata.completion_writes);
        }
        var retried = try owners.consumeNativeIndexSourceAtRoot(alloc, group, plan.before.name, consume_command, .{});
        defer retried.deinit();
        try std.testing.expect(retried.outcome.value.duplicate);
        try std.testing.expectEqual(consumed.outcome.value.position, retried.outcome.value.position);
        var drained = try owners.readNativeIndexClaimSourceAtRoot(alloc, group, plan.before.name, .{ .id = consume_command.binding.id, .plan_digest = consume_command.binding.plan_digest, .owner_digest = consume_command.binding.owner_digest, .ordinal = consume_command.binding.ordinal, .namespace = consume_command.binding.fence.namespace, .owner_group_id = group }, .{});
        defer drained.deinit();
        try std.testing.expect(drained.outcome.batch == null);
        try std.testing.expectEqual(accepted.decision_digest, drained.outcome.consumed.?.accepted.decision_digest);
        try std.testing.expectError(error.PrivateIndexOwnerJobChanged, owners.consumeNativeIndexSourceAtRoot(alloc, group + 1, plan.before.name, consume_command, .{}));
        // Control commands carry the admitted preimage, even when inspection
        // recovered the durable reserved postimage. Never relax that guard.
        canceled_binding.phase = .fencing;
        const intent = native.Admission.fromBinding(canceled_binding);
        metadata.value.head = try metadata.value.head.cancel(metadata.value.head.revision, .requested);
        const cancel_request: execution.CancellationRequest = .{ .id = plan.id, .table_id = plan.before.table_id, .group_id = group };
        var cancel_result = try execution.cancelStep(alloc, &owners, &metadata, plan.before.name, cancel_request, .{});
        defer cancel_result.deinit();
        try std.testing.expect(cancel_result == .intent);
        const canceled = cancel_result.intent.value;
        try std.testing.expect(canceled.position.sequence > reserved_receipt.position.native.sequence);
        try std.testing.expectError(error.PrivateIndexPublicationCanceled, owners.applyNativeIndexOwnerClaims(alloc, group, plan.before.name, claim_command, .{}));
        var manifest = try metadata.value.inputs.?.prepareNativeManifest(alloc, metadata.value.head);
        defer manifest.deinit();
        var canceled_admission = try owners.admitCanceledNativeIndexOwner(alloc, group, plan.before.name, intent, manifest.bytes, .{});
        defer canceled_admission.deinit();
        try std.testing.expect(canceled_admission.value.duplicate);
        inline for (.{ .fenced, .reserved }) |effect| {
            var canceled_control = try owners.applyCanceledNativeIndexOwnerControl(alloc, group, plan.before.name, .{ .effect = effect, .binding = canceled_binding }, .{});
            defer canceled_control.deinit();
            try std.testing.expect(canceled_control.value.duplicate);
            if (effect == .reserved) try std.testing.expectEqual(reserved_receipt.position.native.sequence, canceled_control.value.position.sequence);
        }
        var cleanup_result = try execution.cancelStep(alloc, &owners, &metadata, plan.before.name, cancel_request, .{});
        defer cleanup_result.deinit();
        try std.testing.expect(cleanup_result == .cleanup);
        const cleanup = cleanup_result.cleanup.value;
        try std.testing.expect(cleanup.position.sequence > canceled.position.sequence);
        const owner_identity: native.Identity = .{
            .id = intent.id,
            .plan_digest = intent.plan_digest,
            .owner_digest = intent.owner_digest,
            .ordinal = intent.ordinal,
            .namespace = intent.fence.namespace,
            .owner_group_id = group,
        };
        // GC is time-sliced and may also be advanced by background maintenance.
        // Assert bounded convergence and idempotence, not a fixed page count.
        var retired = false;
        for (0..16) |_| {
            var gc = try execution.cancelStep(alloc, &owners, &metadata, plan.before.name, cancel_request, .{});
            defer gc.deinit();
            if (gc == .retired) {
                retired = true;
                break;
            }
            try std.testing.expect(gc == .gc or gc == .claim_gc);
            if (gc == .gc) try std.testing.expect(gc.gc.value.advanced or gc.gc.value.artifacts_retired);
            if (gc == .claim_gc) try std.testing.expect(gc.claim_gc.value.advanced or gc.claim_gc.value.claims_retired);
        }
        try std.testing.expect(retired);
        var claims = try owners.collectNativeIndexOwnerClaimGarbage(alloc, group, plan.before.name, owner_identity, .{});
        defer claims.deinit();
        try std.testing.expect(!claims.value.advanced and claims.value.claims_retired);
        var repeated = try execution.cancelStep(alloc, &owners, &metadata, plan.before.name, cancel_request, .{});
        defer repeated.deinit();
        try std.testing.expect(repeated == .retired);
        var status = try owners.nativeIndexOwnerCancellationState(alloc, group, plan.before.name, owner_identity, .{});
        defer status.deinit();
        try std.testing.expect(status.value.intent.?.position == .native);
        try std.testing.expectEqual(canceled.position, status.value.intent.?.position.native);
        try std.testing.expectEqual(cleanup.position, status.value.cleanup.?);
        try std.testing.expectEqual(cleanup.position.sequence, status.value.source.sequence);
        try std.testing.expect(status.value.artifacts_retired and status.value.binding != null);
        try std.testing.expect(status.value.claims_retired);
        try std.testing.expect(status.value.claim_allocation != null);
        try std.testing.expectEqual(@as(usize, 1), metadata.admission_reads);
        // Completion is tied to the host signing identity and applying lease.
        const other_root: u128 = if (signing_root.root_incarnation == 1) 2 else 1;
        try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, owners.completeNativeIndexOwnerCancellationAtRoot(alloc, group, plan.before.name, owner_identity, other_root, .{}));
        var completed = try owners.completeNativeIndexOwnerCancellationAtRoot(alloc, group, plan.before.name, owner_identity, signing_root.root_incarnation, .{});
        defer completed.deinit();
        try std.testing.expectEqual(root_generation, completed.root_generation);
        try std.testing.expect(!completed.outcome.value.duplicate and completed.outcome.value.receipt != null);
        try std.testing.expectEqual(.canceled, completed.outcome.value.receipt.?.action);
        try std.testing.expectEqual(cleanup.position.sequence + 1, completed.outcome.value.position.sequence);
        var completion_retry = try owners.completeNativeIndexOwnerCancellationAtRoot(alloc, group, plan.before.name, owner_identity, signing_root.root_incarnation, .{});
        defer completion_retry.deinit();
        try std.testing.expect(completion_retry.outcome.value.duplicate);
        try std.testing.expectEqualDeep(completed.outcome.value.record, completion_retry.outcome.value.record);
        var completed_state = try owners.nativeIndexOwnerCancellationState(alloc, group, plan.before.name, owner_identity, .{});
        defer completed_state.deinit();
        try completed_state.value.validate(owner_identity);
        try std.testing.expectEqualDeep(completed.outcome.value.record, completed_state.value.completed);
        try std.testing.expectEqualSlices(u8, &metadata.value.dispatch.?.native_schema_digest, &completed.outcome.value.record.?.binding.schema_before);
        try std.testing.expectEqualSlices(u8, &metadata.value.dispatch.?.native_inputs_digest, &completed.outcome.value.record.?.binding.inputs_digest);
        try std.testing.expectEqual(metadata.value.dispatch.?.native_claim_allocation, completed.outcome.value.record.?.cleanup.claim_allocation);
        try std.testing.expect(!std.mem.eql(u8, &metadata.value.dispatch.?.native_schema_digest, &metadata.value.dispatch.?.table_guard.owner.schema_digest));
        var signing_completion = try execution.cancelCompletionStep(alloc, &owners, &metadata, plan.before.name, cancel_request, .{});
        defer signing_completion.deinit();
        try std.testing.expect(signing_completion == .completed);
        const value = &signing_completion.completed;
        try std.testing.expect(value.needsSigning());
        try std.testing.expectEqual(@as(?publication.Ack, null), try value.ack());
        value.root_generation += 1;
        try std.testing.expectError(error.IndexOwnerReceiptChanged, value.signNative(signing_root, 2, 3, 4));
        value.root_generation -= 1;
        const retained_receipt = value.outcome.value.receipt;
        value.outcome.value.replication_pending = true;
        value.outcome.value.error_name = "ReplicationOutboxPending";
        value.outcome.value.receipt = null;
        try std.testing.expect(!value.needsSigning());
        try std.testing.expectError(error.NativeIndexSigningUnavailable, value.signNative(signing_root, 2, 3, 4));
        value.outcome.value.replication_pending = false;
        value.outcome.value.error_name = null;
        value.outcome.value.receipt = retained_receipt;
        try value.signNative(signing_root, 2, 3, 4);
        const cleanup_ack = (try value.ack()).?;
        try cleanup_ack.validate();
        try std.testing.expectEqualDeep(completed.outcome.value.record.?, cleanup_ack.cancellation.?.record);
        const retained_proof = try publication.StoredProof.encode(cleanup_ack.signed, null, null, cleanup_ack.cancellation, null);
        try std.testing.expectEqualDeep(cleanup_ack.cancellation, (try publication.StoredProof.decode(alloc, retained_proof.slice())).cancellation);
        var signing_failure = try execution.cancelCompletionStep(alloc, &owners, &metadata, plan.before.name, cancel_request, .{});
        defer signing_failure.deinit();
        signing_failure.completed.signingFailed(.identity_unavailable);
        try std.testing.expect(!signing_failure.completed.needsSigning());
        try std.testing.expectEqual(@as(?publication.Ack, null), try signing_failure.completed.ack());
        try std.testing.expectEqual(completed.outcome.value.position, signing_failure.completed.outcome.value.position);
        const cleanup_progress = try progress.acknowledge(metadata.value.head, metadata.value.owner, metadata.value.head.revision, cleanup_ack.signed.receipt);
        // Resume cancellation through the SAME private control port as normal
        // builds, using a real compiled owner and a synthetic metadata journal.
        var cancellation_scheduler: @import("../metadata/relational_index_worker.zig").Coordinator = .{};
        defer cancellation_scheduler.deinit(std.testing.io);
        var cancellation_now = @import("antfly_platform").time.monotonicNs();
        for (0..80) |_| {
            const before = transport.calls;
            _ = try cancellation_scheduler.step(std.testing.io, alloc, null, &metadata, &local_transport, cancellation_now);
            try std.testing.expect(transport.calls - before <= 1);
            cancellation_now += 300 * std.time.ns_per_ms;
            if (metadata.value.head.phase == .cancel_releasing) break;
        }
        try std.testing.expectEqual(progress.Phase.cancel_releasing, metadata.value.head.phase);
        try std.testing.expectEqualDeep(cleanup_progress.head, metadata.value.head);
        try std.testing.expectEqual(@as(usize, 1), metadata.cleanup_acks);
        const release_intent = metadata.value.cancellation_release.?.intent();
        try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, owners.applyNativeIndexOwnerCancellationReleaseAtRoot(alloc, group, plan.before.name, release_intent, 0, signing_root.root_incarnation, .{}));
        try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, owners.applyNativeIndexOwnerCancellationReleaseAtRoot(alloc, group, plan.before.name, release_intent, root_generation + 1, signing_root.root_incarnation, .{}));
        try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, owners.applyNativeIndexOwnerCancellationReleaseAtRoot(alloc, group, plan.before.name, release_intent, root_generation, other_root, .{}));
        {
            var unchanged = try owners.nativeIndexOwnerCancellationState(alloc, group, plan.before.name, owner_identity, .{});
            defer unchanged.deinit();
            try std.testing.expectEqualDeep(completed.outcome.value.record, unchanged.value.completed);
            try std.testing.expectEqual(completed.outcome.value.position.sequence, unchanged.value.source.sequence);
            try std.testing.expect(unchanged.value.binding != null);
        }
        var released = try owners.applyNativeIndexOwnerCancellationReleaseAtRoot(alloc, group, plan.before.name, release_intent, root_generation, signing_root.root_incarnation, .{});
        defer released.deinit();
        try std.testing.expectEqual(root_generation, released.root_generation);
        try std.testing.expect(!released.outcome.value.duplicate and released.outcome.value.receipt != null);
        try std.testing.expectEqual(.cancel_released, released.outcome.value.receipt.?.action);
        try std.testing.expectEqual(completed.outcome.value.position.sequence + 1, released.outcome.value.position.sequence);
        var release_retry = try owners.applyNativeIndexOwnerCancellationReleaseAtRoot(alloc, group, plan.before.name, release_intent, root_generation, signing_root.root_incarnation, .{});
        defer release_retry.deinit();
        try std.testing.expect(release_retry.outcome.value.duplicate);
        try std.testing.expectEqualDeep(released.outcome.value.receipt, release_retry.outcome.value.receipt);
        try std.testing.expectEqual(released.outcome.value.position, release_retry.outcome.value.position);
        // A caller-chosen digest is not permission, even for a committed cut.
        metadata.value.cancellation_release.?.decision_digest[0] ^= 1;
        try std.testing.expectError(error.InvalidCatalogRecord, execution.cancelReleaseStep(alloc, &owners, &metadata, plan.before.name, cancel_request, .{}));
        metadata.value.cancellation_release.?.decision_digest[0] ^= 1;
        var receiving_release = try execution.cancelReleaseStep(alloc, &owners, &metadata, plan.before.name, cancel_request, .{});
        defer receiving_release.deinit();
        try std.testing.expect(receiving_release == .released and receiving_release.released.outcome.value.duplicate);
        const terminal = &receiving_release.released;
        try std.testing.expect(terminal.needsSigning());
        try std.testing.expectEqual(@as(?publication.Ack, null), try terminal.ack());
        terminal.pinned_root_generation += 1;
        try std.testing.expectError(error.IndexOwnerReceiptChanged, terminal.signNative(signing_root, 2, 3, 4));
        terminal.pinned_root_generation -= 1;
        try terminal.signNative(signing_root, 2, 3, 4);
        const release_ack = (try terminal.ack()).?;
        try release_ack.validate();
        try std.testing.expect(release_ack.cancellation == null);
        try publication.requireCancellationReleaseReceipt(metadata.value.metadata_group_id, metadata.value.metadata_incarnation, metadata.value.head, metadata.value.owner, try publication.StoredProof.decode(alloc, retained_proof.slice()), release_ack.signed, null);
        for (0..100) |_| {
            const before = transport.calls;
            _ = try cancellation_scheduler.step(std.testing.io, alloc, null, &metadata, &local_transport, cancellation_now);
            try std.testing.expect(transport.calls - before <= 1);
            cancellation_now += 300 * std.time.ns_per_ms;
            var resident = false;
            for (cancellation_scheduler.jobs) |job| if (job) |current| {
                for (current.owners) |owner| if (owner != null) {
                    resident = true;
                };
            };
            if (metadata.value.head.phase == .canceled and !resident) break;
        }
        try std.testing.expectEqual(progress.Phase.canceled, metadata.value.head.phase);
        try std.testing.expectEqual(@as(usize, 1), metadata.release_acks);
        try std.testing.expect(metadata.cancellation_status_reads >= 2);
        // A subsequent routing/root switch must not relabel the already
        // recovered receipt as belonging to the replacement physical root.
        root_selection += 1;
        try std.testing.expectEqual(@as(u64, 7), completed.root_generation);
        try std.testing.expectEqual(@as(u64, 7), recovered.recovered.root_generation);
        try std.testing.expectEqual(@as(u64, 7), source_result.rootGeneration().?);
        try std.testing.expectEqual(@as(u64, 7), source_response.signed_source.?.root_generation);
        try std.testing.expectEqual(@as(u64, 7), complete_source.closed.proof.?.root_generation);
    }
}

test "cold warmup reopens transient owners without disturbing resident siblings" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const Catalog = struct {
        fn snapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{
                    .metadata_group_id = 1,
                    .metadata_incarnation = "61616161616161616161616161616161".*,
                    .metrics = .{},
                },
                .tables = @constCast(&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 71,
                    .name = "warmup_probe",
                    .placement_role = "data",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
                }}),
                .ranges = @constCast(&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7501, .table_id = 71, .range_id = 7501, .start_key = "", .end_key = "m" },
                    .{ .group_id = 7502, .table_id = 71, .range_id = 7502, .start_key = "m", .end_key = null },
                }),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        pub fn validate(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
            var current = try snapshot(ptr);
            return contract.matches(&current);
        }
        fn source(self: *@This()) table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{
                .admin_snapshot = snapshot,
                .free_admin_snapshot = free,
                .routing_snapshot = table_catalog.TestAdminRoutingAdapter(snapshot, free).routingSnapshot,
                .linearizable_routing_snapshot = table_catalog.TestAdminRoutingAdapter(snapshot, free).linearizableSnapshot,
                .free_routing_snapshot = table_catalog.TestAdminRoutingAdapter(snapshot, free).freeRoutingSnapshot,
                .validate_publication = validate,
            } };
        }
    };
    var catalog = Catalog{};
    var owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, catalog.source(), read_gate.alreadyReadSafeBarrier());
    defer owners.deinit();
    _ = try owners.reconcileTableGroup(7501, "warmup_probe");
    try std.testing.expectEqual(@as(usize, 1), owners.ownerCountForTest());
    const resident = owners.entries.items[0];
    const misses_before_resident_warmup = owners.cacheStats().miss_count;
    try owners.warmTableGroup(7501, "warmup_probe");
    try std.testing.expectEqual(misses_before_resident_warmup, owners.cacheStats().miss_count);

    for (0..2) |_| {
        const misses_before = owners.cacheStats().miss_count;
        try std.testing.expect((try owners.writeSource().localRuntimeStatusGroupLocal(alloc, 7502, "warmup_probe")) == null);
        try std.testing.expectEqual(misses_before, owners.cacheStats().miss_count);
        try owners.warmTableGroup(7502, "warmup_probe");
        try std.testing.expect(owners.cacheStats().miss_count > misses_before);
        try std.testing.expectEqual(@as(usize, 1), owners.ownerCountForTest());
        try std.testing.expectEqual(resident, owners.entries.items[0]);
        try std.testing.expect((try owners.writeSource().localRuntimeStatusGroupLocal(alloc, 7502, "warmup_probe")) == null);
    }
}

test "bulk callback ABI retains exact consumer error identity" {
    try kernel_owner_source.ProvisionedKernelOwnerSource.validateBulkCallbackIdentityForTest();
}

test "replica retirement cold readers configure a lazy compiled context" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io_impl.io(), ".", alloc);
    defer alloc.free(root);
    const group_path = try std.fs.path.join(alloc, &.{ root, "group-7198" });
    defer alloc.free(group_path);
    inline for (.{ false, true }) |by_path| {
        var owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer owners.deinit();
        try std.testing.expect(owners.context.handle == null);
        const record = if (by_path)
            try owners.readColdInitialChildRetirementRecordAtPath(alloc, group_path)
        else
            try owners.readInitialChildRetirementRecord(alloc, 7198);
        try std.testing.expect(record == null);
        try std.testing.expect(owners.context.handle != null);
        try std.testing.expectEqual(@as(usize, 0), owners.ownerCountForTest());
    }
}

test "replica retirement drains active compiled owners before deleting physical roots" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    inline for (.{ @as(?[]const u8, "retired"), @as(?[]const u8, null) }) |retirement_name| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
        defer alloc.free(root);
        const path = try std.fs.path.join(alloc, &.{ root, "group-7198/table-db" });
        defer alloc.free(path);
        var owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer owners.deinit();
        try std.testing.expectError(error.InvalidArgument, owners.writeSource().retireTableGroupLocal(0, ""));
        var lease = try owners.leaseTransitionOwner(7198, "retired", .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 71, .shard_id = 7198, .range_id = 7198 },
        });
        var lease_owned = true;
        defer if (lease_owned) lease.deinit();
        const identity = try lease.readRelationalTopologyJson(alloc, "identity");
        alloc.free(identity);
        const retained_path = try std.fs.path.join(alloc, &.{ root, "group-7197/table-db" });
        defer alloc.free(retained_path);
        var retained = try owners.leaseTransitionOwner(7197, "retired", .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 71, .shard_id = 7197, .range_id = 7197 },
        });
        retained.deinit();
        try std.testing.expectEqual(@as(usize, 2), owners.ownerCountForTest());
        var source = table_writes.ProvisionedTableWriteSource.init(root, table_catalog.emptyCatalogSource());
        defer source.deinit();
        const Drain = struct {
            owner: @import("antfly_local_sources").api_table_write_source.TableWriteSource,
            io: std.Io,
            entered: std.Io.Event = .unset,
            fn run(ptr: *anyopaque, group_id: u64, name: []const u8) !?void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.entered.set(self.io);
                return self.owner.retireTableGroupLocal(group_id, name);
            }
            fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
                return error.UnexpectedBatch;
            }
        };
        var drain = Drain{ .owner = owners.writeSource(), .io = io };
        _ = source.withLocalWriteSource(.{ .ptr = &drain, .vtable = &.{ .batch = Drain.batch, .retire_table_group_local = Drain.run } });
        const Ownership = struct {
            fn classify(_: *anyopaque, group_id: u64) !table_writes.ProvisionedTableWriteSource.ReplicaRetirementOwnership.State {
                if (group_id != 7198) return error.UnexpectedGroup;
                return .retired;
            }
            fn initialChild(ptr: *anyopaque, allocator: std.mem.Allocator, group_id: u64) !table_writes.InitialChildRetirementObservation {
                const owner: *kernel_owner_source.ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
                // Match the linked runtime's native proof reader. An ordinary
                // replica is removable only after observing no cold AICH;
                // omitting this bridge must continue to fail closed.
                return .{ .record = try owner.readInitialChildRetirementRecord(allocator, group_id) };
            }
        };
        _ = source.withReplicaRetirementOwnership(.{ .ptr = &source, .classify = Ownership.classify });
        _ = source.withInitialChildRetirementReader(.{ .ptr = &owners, .read = Ownership.initialChild });
        var prepared = try source.prepareReplicaRetirements(alloc, &.{.{ .group_id = 7198, .table_name = retirement_name }});
        defer prepared.deinit();
        const Release = struct {
            fn run(wait_io: std.Io, held: *kernel_owner_source.ProvisionedKernelOwnerSource.TransitionLease, physical_path: []const u8, drain_entered: *std.Io.Event) !void {
                defer held.deinit();
                try drain_entered.waitTimeout(wait_io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
                try wait_io.sleep(.fromMilliseconds(50), .awake);
                // A live lease must still own its original files even after
                // the durable retirement has admitted physical cleanup.
                try std.Io.Dir.cwd().access(wait_io, physical_path, .{});
            }
        };
        var releasing = try io.concurrent(Release.run, .{ io, &lease, path, &drain.entered });
        lease_owned = false;
        defer _ = releasing.await(io) catch {};
        try source.completePreparedReplicaRetirements(&prepared);
        try releasing.await(io);
        try std.testing.expectEqual(@as(usize, 1), owners.ownerCountForTest());
        try std.Io.Dir.cwd().access(io, retained_path, .{});
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, path, .{}));
        // Closing the source can no longer recreate WAL/index files beneath
        // the deleted path: all compiled owners were drained before rename.
        owners.deinit();
        owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, path, .{}));
        try std.Io.Dir.cwd().access(io, retained_path, .{});
    }
}

test "empty hidden bootstrap read retries when the root generation advances" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const Generation = struct {
        reads: usize = 0,

        fn visible(ptr: *anyopaque, _: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            return if (self.reads == 1) 1 else 2;
        }
    };
    var generation = Generation{};
    var owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(
        alloc,
        root,
        table_catalog.emptyCatalogSource(),
        read_gate.alreadyReadSafeBarrier(),
    );
    defer owners.deinit();
    _ = owners.withGroupVisibleRootGeneration(.{
        .ptr = &generation,
        .visible_root_generation_for_group = Generation.visible,
    });
    try std.testing.expectError(
        error.StorageKernelOwnerTransitionRequired,
        owners.readHotStandbyHiddenOwnerBootstrap(alloc, 7196, 71),
    );
    try std.testing.expectEqual(@as(usize, 2), generation.reads);
}

test "concurrent cold hidden bootstrap reads share one configured context" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    var owners = kernel_owner_source.ProvisionedKernelOwnerSource.init(
        alloc,
        root,
        table_catalog.emptyCatalogSource(),
        read_gate.alreadyReadSafeBarrier(),
    );
    defer owners.deinit();

    const Worker = struct {
        source: *kernel_owner_source.ProvisionedKernelOwnerSource,
        ready: *std.atomic.Value(usize),
        start: *std.atomic.Value(bool),
        handle: ?*anyopaque = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.ready.fetchAdd(1, .acq_rel);
            while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
            const bootstrap = self.source.readHotStandbyHiddenOwnerBootstrap(std.heap.page_allocator, 7196, 71) catch |err| {
                self.failure = err;
                return;
            };
            if (bootstrap) |value| {
                var parsed = value;
                parsed.deinit();
                self.failure = error.UnexpectedBootstrap;
                return;
            }
            self.handle = self.source.storageContextHandle() catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    const worker_count = 8;
    var ready: std.atomic.Value(usize) = .init(0);
    var start: std.atomic.Value(bool) = .init(false);
    var workers: [worker_count]Worker = undefined;
    var threads: [worker_count]std.Thread = undefined;
    var started: usize = 0;
    errdefer {
        start.store(true, .release);
        for (threads[0..started]) |thread| thread.join();
    }
    for (&workers, &threads) |*worker, *thread| {
        worker.* = .{ .source = &owners, .ready = &ready, .start = &start };
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
        started += 1;
    }
    while (ready.load(.acquire) != worker_count) std.atomic.spinLoopHint();
    start.store(true, .release);
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    const expected = workers[0].handle orelse return error.MissingStorageContext;
    for (workers) |worker| {
        if (worker.failure) |err| return err;
        try std.testing.expectEqual(expected, worker.handle orelse return error.MissingStorageContext);
    }
}

test "hidden constrained lookup recovers cold compiled owner from exact plan authority" {
    const alloc = std.testing.allocator;
    const Source = kernel_owner_source.ProvisionedKernelOwnerSource;
    const staging = @import("antfly_local_sources").storage_db_restore_staging_contract;
    const schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"unique_constraints\":[{\"name\":\"pk\",\"columns\":[\"id\"]}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    const tables = @import("../api/tables.zig");
    var parsed = try tables.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try tables.deriveRuntimeTableSchema(alloc, parsed);
    defer @import("antfly_local_sources").storage_schema.freeSchema(alloc, schema);
    const encoded_schema = try @import("antfly_local_sources").storage_schema.serializeSchema(alloc, schema);
    defer alloc.free(encoded_schema);
    const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = .{ .table_id = 70, .shard_id = 7001, .range_id = 7001 }, .target_namespace = .{ .table_id = 71, .shard_id = 7196, .range_id = 7196 }, .target_schema_digest = staging.digest(encoded_schema) };
    const bootstrap: staging.OwnerBootstrap = .{ .scope = scope, .table_name = "hidden", .schema_json = schema_json, .indexes_json = "{}", .byte_range = .{ .start = "a", .end = "m" } };
    const bootstrap_json = try std.json.Stringify.valueAlloc(alloc, bootstrap, .{});
    defer alloc.free(bootstrap_json);
    const descriptor: @import("kernel_owner_descriptor.zig").Descriptor = .{ .lsm_root_generation = table_reads.backend_current_root_generation, .identity = .{ .table_id = 71, .shard_id = 7196, .range_id = 7196 }, .schema_json = schema_json, .indexes_json = "{}", .restore_bootstrap_json = bootstrap_json };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const Authority = struct {
        descriptor: @import("kernel_owner_descriptor.zig").Descriptor,
        expected: staging.Scope,
        denied: bool = true,
        expected_use: Source.RestoreDescriptorUse = .read,
        reads: usize = 0,
        barrier_blocked: bool = false,
        barriers: usize = 0,
        fn recover(ptr: *anyopaque, allocator: std.mem.Allocator, group_id: u64, name: []const u8, digest: [32]u8, plan_id: [16]u8, use: Source.RestoreDescriptorUse, context: @import("antfly_local_sources").api_operation.RequestContext) !Source.OwnedRestoreDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            try std.testing.expectEqual(@as(u64, 7196), group_id);
            try std.testing.expectEqualStrings("hidden", name);
            try std.testing.expectEqual(self.expected.digest(), digest);
            try std.testing.expectEqual(self.expected.plan_id, plan_id);
            try std.testing.expectEqual(self.expected_use, use);
            self.reads += 1;
            if (self.denied) return error.RestoreStagingCanceled;
            const schema_owned = try allocator.dupe(u8, self.descriptor.schema_json);
            errdefer allocator.free(schema_owned);
            const indexes_owned = try allocator.dupe(u8, self.descriptor.indexes_json);
            errdefer allocator.free(indexes_owned);
            const bootstrap_owned = try allocator.dupe(u8, self.descriptor.restore_bootstrap_json);
            var result = self.descriptor;
            result.schema_json = schema_owned;
            result.indexes_json = indexes_owned;
            result.restore_bootstrap_json = bootstrap_owned;
            return .{ .descriptor = result };
        }
        fn barrier(ptr: *anyopaque, group_id: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7196), group_id);
            self.barriers += 1;
            if (self.barrier_blocked) return error.NotLeader;
        }
    };
    var authority = Authority{ .descriptor = descriptor, .expected = scope };
    const barrier: read_gate.ReadSafetyBarrier = .{ .ptr = &authority, .vtable = &.{ .wait_read_safe = Authority.barrier } };
    var owners = Source.init(alloc, root, table_catalog.emptyCatalogSource(), barrier);
    defer owners.deinit();
    var stale_descriptor = descriptor;
    stale_descriptor.lsm_root_generation +%= 1;
    try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, owners.primeRestoreOwner(7196, "hidden", stale_descriptor));
    try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, owners.restoreOwnerControl(alloc, 7196, "hidden", stale_descriptor, .{ .scope = scope, .action = .begin }, null, .{}, .{}));
    _ = try owners.restoreOwnerControl(alloc, 7196, "hidden", descriptor, .{ .scope = scope, .action = .begin }, null, .{}, .{});
    const before = try (staging.Progress{ .scope = scope, .phase = .importing, .source_generation_proofs_complete = true }).encode(alloc);
    defer alloc.free(before);
    const imported = try (staging.Progress{ .scope = scope, .phase = .imported, .source_generation_proofs_complete = true }).encode(alloc);
    defer alloc.free(imported);
    try owners.applyPreparedReplicatedBatchGroupLocal(alloc, 7196, "hidden", descriptor, .{
        .restore_staging = .{ .import_page = .{ .expected = staging.digest(before), .next = imported, .scope = scope.digest(), .timestamps = &.{} } },
    });
    owners.deinit();
    owners = Source.init(alloc, root, table_catalog.emptyCatalogSource(), barrier);
    _ = owners.withRestoreDescriptorRecovery(.{ .ptr = &authority, .recover_fn = Authority.recover });
    authority.denied = false;
    authority.descriptor = stale_descriptor;
    try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, owners.resolveRestoreDescriptor(alloc, 7196, "hidden", scope.digest(), scope.plan_id, .read, .{}));
    authority.descriptor = descriptor;
    authority.denied = true;
    var options: db_mod.types.LookupOptions = .{ .restore_staging_scope = scope.digest(), .relational_activation_json = "{\"mode\":\"status\"}" };
    try std.testing.expectError(error.RestoreStagingScopeChanged, owners.readSource().lookupGroupLocal(alloc, 7196, "hidden", "a", options, .read_index));
    options.restore_staging_plan_id = scope.plan_id;
    try std.testing.expectError(error.RestoreStagingCanceled, owners.readSource().lookupGroupLocal(alloc, 7196, "hidden", "a", options, .read_index));
    try std.testing.expectEqual(@as(usize, 0), owners.ownerCountForTest());
    authority.denied = false;
    authority.barrier_blocked = true;
    try std.testing.expectError(error.NotLeader, owners.readSource().lookupGroupLocal(alloc, 7196, "hidden", "a", options, .stale));
    try std.testing.expectEqual(@as(usize, 0), owners.ownerCountForTest());
    authority.barrier_blocked = false;
    var status = (try owners.readSource().lookupGroupLocal(alloc, 7196, "hidden", "a", options, .stale)).?;
    defer status.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, status.json, "state") != null);
    try std.testing.expect(authority.barriers >= 2);
    options.restore_staging_plan_id = @splat(9);
    try std.testing.expectError(error.RestoreStagingScopeChanged, owners.readSource().lookupGroupLocal(alloc, 7196, "hidden", "a", options, .read_index));
    try std.testing.expectError(error.TableNotFound, owners.readSource().lookupGroupLocal(alloc, 7196, "hidden", "a", .{}, .read_index));
    // The direct compiled transaction adapter must not drop private authority
    // from pre-decision context, and status must recover it after another cold
    // owner restart without relying on an earlier read to prime the cache.
    const txn: db_mod.types.TxnId = @splat(0x71);
    const participant = try @import("../api/distributed_txn.zig").participantIdForGroupScoped(alloc, "hidden", 7196, scope.digest(), scope.plan_id);
    defer alloc.free(participant);
    owners.deinit();
    owners = Source.init(alloc, root, table_catalog.emptyCatalogSource(), barrier);
    _ = owners.withRestoreDescriptorRecovery(.{ .ptr = &authority, .recover_fn = Authority.recover });
    authority.expected_use = .mutate;
    _ = try owners.writeSource().txnBeginGroupLocalWithPreDecisionContext(alloc, 7196, "hidden", txn, 1, 1, true, &.{participant}, .{ .restore_staging_scope = scope.digest(), .restore_staging_plan_id = scope.plan_id });
    owners.deinit();
    owners = Source.init(alloc, root, table_catalog.emptyCatalogSource(), barrier);
    _ = owners.withRestoreDescriptorRecovery(.{ .ptr = &authority, .recover_fn = Authority.recover });
    authority.expected_use = .resolve;
    const status_request: @import("antfly_local_sources").api_distributed_txn_contract.TxnStatusRequest = .{ .txn_id = txn, .restore_staging_scope = scope.digest(), .restore_staging_plan_id = scope.plan_id };
    authority.barrier_blocked = true;
    try std.testing.expectError(error.NotLeader, owners.writeSource().txnStatusGroupLocalWithRequest(alloc, 7196, "hidden", status_request, .{}));
    try std.testing.expectEqual(@as(usize, 0), owners.ownerCountForTest());
    authority.barrier_blocked = false;
    try std.testing.expectEqual(db_mod.types.TxnStatus.pending, (try owners.writeSource().txnStatusGroupLocalWithRequest(alloc, 7196, "hidden", status_request, .{})).?);
    var wrong_status = status_request;
    wrong_status.restore_staging_plan_id = @splat(0xaa);
    try std.testing.expectError(error.RestoreStagingScopeChanged, owners.writeSource().txnStatusGroupLocalWithRequest(alloc, 7196, "hidden", wrong_status, .{}));
    wrong_status = status_request;
    wrong_status.restore_staging_scope = @splat(0xbb);
    try std.testing.expectError(error.RestoreStagingScopeChanged, owners.writeSource().txnStatusGroupLocalWithRequest(alloc, 7196, "hidden", wrong_status, .{}));
    // Restore validation uses scoped batch controls, not the direct native
    // transaction callbacks above. Exercise both decisions through that route
    // and lose their successful reply across a cold owner restart.
    for ([_]db_mod.types.TxnStatus{ .aborted, .committed }, 0..) |decision, attempt| {
        authority.expected_use = .mutate;
        const batch_txn: db_mod.types.TxnId = @splat(@as(u8, @intCast(0x72 + attempt)));
        const begin: db_mod.types.BatchRequest = .{
            .restore_staging_scope = scope.digest(),
            .restore_staging_plan_id = scope.plan_id,
            .transaction = .{ .begin = .{ .txn_id = batch_txn, .begin_timestamp = 10, .created_at_ns = 10, .topology_epoch = 1, .retain_terminal = true, .participants = &.{participant} } },
        };
        var stale_begin = begin;
        stale_begin.restore_staging_plan_id = @splat(0xaa);
        try std.testing.expectError(error.RestoreStagingScopeChanged, owners.writeSource().batchGroupLocal(alloc, 7196, "hidden", stale_begin));
        _ = (try owners.writeSource().batchGroupLocal(alloc, 7196, "hidden", begin)) orelse return error.TestUnexpectedResult;
        _ = (try owners.writeSource().batchGroupLocal(alloc, 7196, "hidden", .{
            .restore_staging_scope = scope.digest(),
            .restore_staging_plan_id = scope.plan_id,
            .transaction = .{ .prepare = .{ .txn_id = batch_txn, .topology_epoch = 1 } },
        })) orelse return error.TestUnexpectedResult;
        const resolve: db_mod.types.BatchRequest = .{
            .restore_staging_scope = scope.digest(),
            .restore_staging_plan_id = scope.plan_id,
            .transaction = .{ .resolve = .{ .txn_id = batch_txn, .status = decision, .commit_version = 11 } },
        };
        authority.expected_use = .resolve;
        _ = (try owners.writeSource().batchGroupLocal(alloc, 7196, "hidden", resolve)) orelse return error.TestUnexpectedResult;
        owners.deinit();
        owners = Source.init(alloc, root, table_catalog.emptyCatalogSource(), barrier);
        _ = owners.withRestoreDescriptorRecovery(.{ .ptr = &authority, .recover_fn = Authority.recover });
        authority.expected_use = .resolve;
        _ = (try owners.writeSource().batchGroupLocal(alloc, 7196, "hidden", resolve)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(decision, (try owners.writeSource().txnStatusGroupLocalWithRequest(alloc, 7196, "hidden", .{
            .txn_id = batch_txn,
            .restore_staging_scope = scope.digest(),
            .restore_staging_plan_id = scope.plan_id,
        }, .{})).?);
        try std.testing.expectError(error.TableNotFound, owners.readSource().lookupGroupLocal(alloc, 7196, "hidden", "a", .{}, .read_index));
    }
}

test "restore publication recovers a cold compiled owner between preparation and admission" {
    const alloc = std.testing.allocator;
    const Source = kernel_owner_source.ProvisionedKernelOwnerSource;
    const staging = @import("antfly_local_sources").storage_db_restore_staging_contract;
    const schema_json = "{}";
    const tables = @import("../api/tables.zig");
    var parsed = try tables.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try tables.deriveRuntimeTableSchema(alloc, parsed);
    defer @import("antfly_local_sources").storage_schema.freeSchema(alloc, schema);
    const encoded_schema = try @import("antfly_local_sources").storage_schema.serializeSchema(alloc, schema);
    defer alloc.free(encoded_schema);
    const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = .{ .table_id = 70, .shard_id = 7001, .range_id = 7001 }, .target_namespace = .{ .table_id = 71, .shard_id = 7196, .range_id = 7196 }, .target_schema_digest = staging.digest(encoded_schema) };
    const bootstrap: staging.OwnerBootstrap = .{ .scope = scope, .table_name = "hidden", .schema_json = schema_json, .indexes_json = "{}", .byte_range = .{ .start = "a", .end = "m" } };
    const bootstrap_json = try std.json.Stringify.valueAlloc(alloc, bootstrap, .{});
    defer alloc.free(bootstrap_json);
    const descriptor: @import("kernel_owner_descriptor.zig").Descriptor = .{ .lsm_root_generation = table_reads.backend_current_root_generation, .identity = .{ .table_id = 71, .shard_id = 7196, .range_id = 7196 }, .schema_json = schema_json, .indexes_json = "{}", .restore_bootstrap_json = bootstrap_json };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const Authority = struct {
        descriptor: @import("kernel_owner_descriptor.zig").Descriptor,
        expected: staging.Scope,
        expected_use: Source.RestoreDescriptorUse = .resolve,
        reads: usize = 0,
        fn recover(ptr: *anyopaque, allocator: std.mem.Allocator, group_id: u64, name: []const u8, digest: [32]u8, plan_id: [16]u8, use: Source.RestoreDescriptorUse, context: @import("antfly_local_sources").api_operation.RequestContext) !Source.OwnedRestoreDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            try std.testing.expectEqual(@as(u64, 7196), group_id);
            try std.testing.expectEqualStrings("hidden", name);
            try std.testing.expectEqual(self.expected.digest(), digest);
            try std.testing.expectEqual(self.expected.plan_id, plan_id);
            try std.testing.expectEqual(self.expected_use, use);
            self.reads += 1;
            const schema_owned = try allocator.dupe(u8, self.descriptor.schema_json);
            errdefer allocator.free(schema_owned);
            const indexes_owned = try allocator.dupe(u8, self.descriptor.indexes_json);
            errdefer allocator.free(indexes_owned);
            const bootstrap_owned = try allocator.dupe(u8, self.descriptor.restore_bootstrap_json);
            var result = self.descriptor;
            result.schema_json = schema_owned;
            result.indexes_json = indexes_owned;
            result.restore_bootstrap_json = bootstrap_owned;
            return .{ .descriptor = result };
        }
        fn barrier(_: *anyopaque, _: u64, _: []const u8) !void {}
    };
    var authority = Authority{ .descriptor = descriptor, .expected = scope };
    const barrier: read_gate.ReadSafetyBarrier = .{ .ptr = &authority, .vtable = &.{ .wait_read_safe = Authority.barrier } };
    var owners = Source.init(alloc, root, table_catalog.emptyCatalogSource(), barrier);
    defer owners.deinit();
    _ = try owners.restoreOwnerControl(alloc, 7196, "hidden", descriptor, .{ .scope = scope, .action = .begin }, null, .{}, .{});
    const before = try (staging.Progress{ .scope = scope, .phase = .importing, .source_generation_proofs_complete = true }).encode(alloc);
    defer alloc.free(before);
    const imported = try (staging.Progress{ .scope = scope, .phase = .imported, .source_generation_proofs_complete = true }).encode(alloc);
    defer alloc.free(imported);
    try owners.applyPreparedReplicatedBatchGroupLocal(alloc, 7196, "hidden", descriptor, .{
        .restore_staging = .{ .import_page = .{ .expected = staging.digest(before), .next = imported, .scope = scope.digest(), .timestamps = &.{} } },
    });
    // Lose the hidden descriptor between preparation and Raft admission.
    // The captured finish must carry its plan identity and recover under
    // resolution authority after metadata has already published the cohort.
    _ = try owners.restoreOwnerControl(alloc, 7196, "hidden", descriptor, .{ .scope = scope, .action = .validate }, null, .{}, .{});
    const Publication = struct {
        owners: *Source,
        authority: *Authority,
        root: []const u8,
        barrier: read_gate.ReadSafetyBarrier,
        fn propose(ptr: *anyopaque, request: db_mod.types.BatchRequest, context: @import("antfly_local_sources").api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            try std.testing.expectEqual(self.authority.expected.plan_id, request.restore_staging_plan_id.?);
            try std.testing.expectEqual(self.authority.expected.digest(), request.restore_staging_scope.?);
            self.owners.deinit();
            self.owners.* = Source.init(std.testing.allocator, self.root, table_catalog.emptyCatalogSource(), self.barrier);
            _ = self.owners.withRestoreDescriptorRecovery(.{ .ptr = self.authority, .recover_fn = Authority.recover });
            self.authority.expected_use = .resolve;
            _ = (try self.owners.writeSource().batchGroupLocal(std.testing.allocator, 7196, "hidden", request)) orelse return error.TestUnexpectedResult;
        }
    };
    var publication = Publication{ .owners = &owners, .authority = &authority, .root = root, .barrier = barrier };
    const published = try owners.restoreOwnerControl(alloc, 7196, "hidden", descriptor, .{ .scope = scope, .action = .publish }, .{ .ptr = &publication, .propose = Publication.propose }, .{}, .{});
    try std.testing.expectEqual(staging.Phase.published, published.phase);
    try std.testing.expectEqual(@as(usize, 1), authority.reads);
}

test "transition lease reads unpublished owner metadata without admitting document reads" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    // This fixture deliberately has no public routing entry for the owner.
    // Only the admitted transition descriptor is allowed to open it.
    var source = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    var lease = try source.leaseTransitionOwner(7199, "unpublished", .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 71, .shard_id = 7199, .range_id = 7199 },
    });
    defer lease.deinit();
    const topology = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract;
    const identity_json = try lease.readRelationalTopologyJson(alloc, "identity");
    defer alloc.free(identity_json);
    var identity = try std.json.parseFromSlice(topology.Identity, alloc, identity_json, .{});
    defer identity.deinit();
    try std.testing.expectEqual(@as(u64, 71), identity.value.namespace.table_id);
    try std.testing.expectEqual(@as(u64, 7199), identity.value.namespace.shard_id);
    const status_json = try lease.readRelationalTopologyJson(alloc, "status");
    defer alloc.free(status_json);
    var status = try std.json.parseFromSlice(topology.Status, alloc, status_json, .{});
    defer status.deinit();
    try std.testing.expect(status.value.drained and status.value.fence == null);
    // Exercise the compiled transition lease, not only native DB lookup: its
    // lifecycle allowlist must accept the same receipt mode as the wire.
    const receipt_json = try lease.readRelationalTopologyJson(alloc, "merge_copy_receipt");
    defer alloc.free(receipt_json);
    var receipt = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_merge_contract.CopyReceipt, alloc, receipt_json, .{});
    defer receipt.deinit();
    try std.testing.expectEqual(identity.value.namespace, receipt.value.namespace);
    try std.testing.expect(receipt.value.state == null);
    // An uninitialized owner is not affirmative document-copy authority.
    try std.testing.expect(!receipt.value.row_derived_document);
    try std.testing.expectError(error.InvalidArgument, lease.readRelationalTopologyJson(alloc, "document"));
    try std.testing.expectError(error.InvalidArgument, lease.readRelationalTopologyJson(alloc, ""));
    try std.testing.expectError(error.TableNotFound, source.readSource().lookupGroupLocal(alloc, 7199, "unpublished", "private-row", .{}, .read_index));
}

test "restore owner admission cannot pin an unimported replica generation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/replicas", .{tmp.sub_path});
    defer alloc.free(root);
    const Catalog = struct {
        restoring: bool = true,

        fn snapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = "{}" }}),
                .ranges = if (!self.restoring) @constCast(&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }}) else @constCast(&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                    .restore_backup_id = "backup",
                    .restore_location = "file:///backup",
                    .restore_snapshot_path = "groups/1.afb",
                    .restore_artifact_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                }}),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        fn freeSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    var catalog = Catalog{};
    var source = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, root, .{
        .ptr = &catalog,
        .vtable = &.{ .admin_snapshot = Catalog.snapshot, .free_admin_snapshot = Catalog.freeSnapshot },
    }, read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    // Force the production history: catalog restore intent is visible before
    // Raft bootstrap imports the generation. Background repair must not create
    // an empty resident DB that prevents bootstrap's exclusive publication.
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.reconcileTableGroup(7001, "docs"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.warmTableGroup(7001, "docs"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.restoreState(alloc, 7001, "docs"));
    try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());

    // An owner admitted before the restore intent must not bypass the proof
    // when the catalog changes. Retirement releases its publication lease.
    catalog.restoring = false;
    _ = try source.reconcileTableGroup(7001, "docs");
    try std.testing.expectEqual(@as(usize, 1), source.ownerCountForTest());
    catalog.restoring = true;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.warmTableGroup(7001, "docs"));
    try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());
}

test "provisioned batch lookup scan and query share one opaque live storage owner" {
    const alloc = std.testing.allocator;
    var replica_tmp = std.testing.tmpDir(.{});
    defer replica_tmp.cleanup();
    var backup_tmp = std.testing.tmpDir(.{});
    defer backup_tmp.cleanup();
    const replica_root = try replica_tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(replica_root);
    const backup_root = try backup_tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(backup_root);

    const Catalog = struct {
        const metadata_incarnation: metadata_api.MetadataClusterIncarnation = "31313131313131313131313131313131".*;
        const indexes_json =
            \\{"dense_idx":{"type":"embeddings","external":true,"dimension":3},
            \\ "full_text_index_v0":{"type":"full_text","enrichments":[{"name":"document_units_v1","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"}]},
            \\ "alg":{"type":"algebraic","version":1,"table":"articles","schema_version":1,
            \\        "group_fields":[{"name":"category","path":"category","type":"keyword"}],
            \\        "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
            \\        "materializations":[{"name":"sum_by_category","op":"sum","group_by":["category"],"measure":"amount"}]},
            \\ "relations_graph":{"type":"graph","edge_types":[{"name":"mentions"}],"metrics":{"degree":{"enabled":true,"kind":"degree","refresh":"manual"}}}}
        ;

        accept_publication: bool = true,
        present: bool = true,
        include_cold_group: bool = false,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .routing_snapshot = table_catalog.TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).routingSnapshot,
                    .linearizable_routing_snapshot = table_catalog.TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).linearizableSnapshot,
                    .table_routing_snapshot = descriptorSnapshot,
                    .linearizable_table_routing_snapshot = descriptorSnapshot,
                    .free_routing_snapshot = table_catalog.TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).freeRoutingSnapshot,
                    .validate_publication = validatePublication,
                },
            };
        }

        fn descriptorSnapshot(ptr: *anyopaque, _: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            return table_catalog.TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).routingSnapshot(ptr, deadline_ns);
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var result: metadata_api.AdminSnapshot = .{
                .status = .{
                    .metadata_group_id = 1,
                    .metadata_incarnation = metadata_incarnation,
                    .metrics = .{},
                },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "articles",
                    .placement_role = "data",
                    .indexes_json = indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 7001,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
            if (!self.present) {
                result.tables = &.{};
                result.ranges = &.{};
            } else if (self.include_cold_group) {
                result.ranges = @constCast(&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .range_id = 7001, .start_key = "", .end_key = "m" },
                    .{ .group_id = 7002, .table_id = 7, .range_id = 7002, .start_key = "m", .end_key = null },
                });
            }
            return result;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn validatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.accept_publication) return false;
            var snapshot = try adminSnapshot(ptr);
            return contract.matches(&snapshot);
        }
    };

    const LeaseCapture = struct {
        count: usize = 0,
        last_group_id: u64 = 0,

        fn barrier(self: *@This()) read_gate.ReadSafetyBarrier {
            return .{
                .ptr = self,
                .vtable = &.{ .wait_read_safe = waitReadSafe },
            };
        }

        fn waitReadSafe(ptr: *anyopaque, group_id: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
            self.last_group_id = group_id;
        }
    };

    const GenerationTracker = struct {
        generation: u64 = table_reads.backend_current_root_generation,
        reservations: usize = 0,

        fn iface(self: *@This()) table_reads.GroupVisibleRootGenerationSource {
            return .{
                .ptr = self,
                .visible_root_generation_for_group = visible,
                .reserve_root_generation_for_group = reserve,
                .finish_root_generation_reservation = finish,
            };
        }

        fn visible(ptr: *anyopaque, _: u64) u64 {
            return (@as(*@This(), @ptrCast(@alignCast(ptr)))).generation;
        }

        fn reserve(ptr: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reservations += 1;
        }

        fn finish(ptr: *anyopaque, _: u64, advance: bool) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.reservations > 0);
            if (advance) self.generation +%= 1;
            self.reservations -= 1;
        }
    };

    var snapshot_cache = @import("antfly_local_sources").api_runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var catalog = Catalog{};
    var lease_capture = LeaseCapture{};
    var generations = GenerationTracker{};
    var owner_source = kernel_owner_source.ProvisionedKernelOwnerSource.init(
        alloc,
        replica_root,
        catalog.iface(),
        lease_capture.barrier(),
    );
    _ = owner_source.withGroupVisibleRootGeneration(generations.iface());
    _ = owner_source.withRuntimeStatusCache(&snapshot_cache);
    var owner_source_active = true;
    defer if (owner_source_active) owner_source.deinit();
    var write_source = table_writes.ProvisionedTableWriteSource.init(replica_root, catalog.iface());
    write_source.runtime_status_cache = &snapshot_cache;
    var write_source_active = true;
    defer if (write_source_active) write_source.deinit();
    _ = owner_source.withTransactionRecoverySource(write_source.transactionRecoverySource());
    _ = write_source.withLocalWriteSource(owner_source.writeSource());
    _ = write_source.withStorageSnapshotSource(owner_source.snapshotSource());
    _ = write_source.withStorageMaintenanceSource(owner_source.maintenanceSource());
    _ = write_source.withGroupVisibleRootGeneration(generations.iface());
    var read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root,
        catalog.iface(),
        lease_capture.barrier(),
    );
    _ = read_source.withLocalReadSource(owner_source.readSource());
    _ = read_source.withGroupVisibleRootGeneration(generations.iface());

    try std.testing.expect((try write_source.source().createTable(alloc, "articles", .{})) != null);
    {
        // A guarded merge copy pins the transition descriptor without the
        // catalog's initial-range hint. Its final donor proposal must drop
        // only that native pin before ordinary admission reacquires the exact
        // catalog descriptor, retaining the transition admission gate.
        var descriptor = try owner_source.loadDescriptor(alloc, 7001, "articles");
        defer descriptor.deinit(alloc);
        try std.testing.expect(descriptor.initial_range != null);
        var transition_descriptor = descriptor.view();
        transition_descriptor.initial_range = null;
        var activity = write_source.beginGroupTransitionActivity("articles", 7001);
        defer activity.deinit();
        var copied_owner = try owner_source.leaseTransitionOwner(7001, "articles", transition_descriptor);
        var pinned = true;
        defer if (pinned) copied_owner.deinit();
        try std.testing.expectError(error.StorageBusy, owner_source.writeSource().preflightWriteAdmissionGroupLocal(7001, "articles"));
        copied_owner.deinit();
        pinned = false;
        try std.testing.expect(write_source.hasReadBlockingActivityBestEffort("articles", 7001));
        try std.testing.expect((try owner_source.writeSource().preflightWriteAdmissionGroupLocal(7001, "articles")) != null);
        try std.testing.expect(write_source.hasReadBlockingActivityBestEffort("articles", 7001));
    }
    {
        const response = (try write_source.source().graphMetricAction(alloc, "articles", "relations_graph", "degree", "pause")) orelse return error.ExpectedGraphMetricStatus;
        var status = response;
        defer status.deinit(alloc);
        try std.testing.expect(status.maintenance_paused);
        const resumed = (try write_source.source().graphMetricAction(alloc, "articles", "relations_graph", "degree", "resume")) orelse return error.ExpectedGraphMetricStatus;
        var resumed_status = resumed;
        defer resumed_status.deinit(alloc);
        try std.testing.expect(!resumed_status.maintenance_paused);
    }
    // A storage transition can outlive a request. Admission must stop waiting
    // on the request's controls, without altering the transition's ownership.
    const Admission = struct {
        fn expectTimeout(outcome: anytype) !void {
            // One deadline covers catalog projection and owner admission. A
            // busy host can exhaust it in either phase; retain each phase's
            // diagnostic identity rather than assuming catalog capture fits
            // inside this deliberately short real-time budget. Fake-clock
            // tests separately verify the exact catalog error and budget.
            if (outcome) |_| return error.TestExpectedError else |err| switch (err) {
                error.Timeout, error.CatalogRoutingSnapshotTimeout => {},
                else => return err,
            }
        }
        fn expectCancellation(outcome: anytype) !void {
            if (outcome) |_| return error.TestExpectedError else |err| switch (err) {
                error.Cancelled, error.Canceled => {},
                else => return err,
            }
        }
    };
    {
        const time = @import("antfly_platform").time;
        const entry = owner_source.entries.items[0];
        entry.exclusive_active = true;
        defer entry.exclusive_active = false;
        const started = time.monotonicNs();
        try Admission.expectTimeout(owner_source.readSource().queryGroupLocal(alloc, 7001, "articles", .{
            .execution_deadline_ns = started + 20 * std.time.ns_per_ms,
        }, .stale));
        try std.testing.expect(time.monotonicNs() - started < 4 * std.time.ns_per_s);
        try std.testing.expect(entry.exclusive_active);
        try Admission.expectTimeout(owner_source.readSource().lookupGroupLocal(alloc, 7001, "articles", "doc:1", .{
            .execution_deadline_ns = time.monotonicNs() + 20 * std.time.ns_per_ms,
        }, .stale));
        try std.testing.expect(entry.exclusive_active);
        const CancelAfter = struct {
            at: u64,
            fn requested(ptr: *const anyopaque) bool {
                const self: *const @This() = @ptrCast(@alignCast(ptr));
                return @import("antfly_platform").time.monotonicNs() >= self.at;
            }
        };
        const cancel = CancelAfter{ .at = time.monotonicNs() + 20 * std.time.ns_per_ms };
        try Admission.expectCancellation(owner_source.readSource().queryGroupLocal(alloc, 7001, "articles", .{
            .cancellation = .{ .ptr = &cancel, .is_cancelled_fn = CancelAfter.requested },
        }, .stale));
        try std.testing.expect(entry.exclusive_active);
    }
    // Registry contention must use the same interruptible wait, not a spin lock
    // that prevents the request from reaching its cancellation checks.
    {
        try std.testing.expect(owner_source.mutex.tryLock());
        defer owner_source.mutex.unlock();
        try Admission.expectTimeout(owner_source.readSource().queryGroupLocal(alloc, 7001, "articles", .{
            .execution_deadline_ns = @import("antfly_platform").time.monotonicNs() + 20 * std.time.ns_per_ms,
        }, .stale));
    }

    // Background publication uses only existing owners and does not open a DB.
    const owner_count = (try owner_source.maintenanceSource().maintenanceSnapshot(false)).owner_count;
    write_source.publishCachedWriterRuntimeStatusesBestEffort(alloc);
    try std.testing.expectEqual(owner_count, (try owner_source.maintenanceSource().maintenanceSnapshot(false)).owner_count);
    {
        var published = (try snapshot_cache.snapshot(alloc, "articles")).?;
        defer published.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), published.items.len);
        try std.testing.expectEqual(@as(u64, 7001), published.items[0].group_id);
        try std.testing.expectEqual(@import("antfly_local_sources").api_runtime_status.RuntimeStatusFreshness.fresh, published.items[0].metadata.freshness);
    }

    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    // A delayed startup warmup must not retire the owner already installed by
    // foreground work or startup catch-up. Its workers and future observations
    // belong to that resident owner even when no API lease is currently held.
    const before_warmup = owner_source.cacheStats();
    const resident_owner = owner_source.entries.items[0];
    try owner_source.warmTableGroup(7001, "articles");
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expectEqual(resident_owner, owner_source.entries.items[0]);
    try std.testing.expectEqual(before_warmup.miss_count, owner_source.cacheStats().miss_count);
    {
        // Start with the complete two-range catalog, retaining only one owner.
        // Adding the sibling to the original fixture would also change its
        // resident range from ["", infinity) to ["", "m") without a topology
        // transition. Status correctly refuses that stale descriptor.
        var status_tmp = std.testing.tmpDir(.{});
        defer status_tmp.cleanup();
        const status_root = try status_tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
        defer alloc.free(status_root);
        var status_catalog = Catalog{ .include_cold_group = true };
        var status_source = kernel_owner_source.ProvisionedKernelOwnerSource.init(alloc, status_root, status_catalog.iface(), read_gate.alreadyReadSafeBarrier());
        defer status_source.deinit();
        _ = try status_source.reconcileTableGroup(7001, "articles");
        var partial_status = (try status_source.writeSource().localRuntimeStatuses(alloc, "articles")) orelse return error.ExpectedResidentGroupObservation;
        defer partial_status.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), partial_status.items.len);
        try std.testing.expectEqual(@as(u64, 7001), partial_status.items[0].group_id);
        try std.testing.expectEqual(@as(usize, 1), status_source.ownerCountForTest());
        const status_owner = status_source.entries.items[0];
        status_owner.exclusive_active = true;
        defer status_owner.exclusive_active = false;
        try std.testing.expect((try status_source.writeSource().localRuntimeStatuses(alloc, "articles")) == null);
    }
    // A compiled owner must return its post-reconcile observation to the
    // control-plane publisher. Completing the DB mutation without those facts
    // leaves exact activation retrying EmptyTargetedIndexObservation forever.
    var before_activation = (try snapshot_cache.snapshot(alloc, "articles")).?;
    defer before_activation.deinit(alloc);
    const before_status = before_activation.items[0];
    const expected_index = for (before_status.stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, "dense_idx")) break item;
    } else return error.ExpectedDenseIndex;
    _ = try write_source.source().createIndex(alloc, "articles", "dense_idx", "{\"type\":\"embeddings\",\"external\":true,\"dimension\":3}");
    {
        const time = @import("antfly_platform").time;
        const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        while (write_source.structural_reconcile_scheduled.load(.acquire) and time.monotonicNs() < deadline)
            try io_impl.io().sleep(.fromMilliseconds(5), .awake);
        try std.testing.expect(!write_source.structural_reconcile_scheduled.load(.acquire));
        // Completion requires fresh publication of this exact incarnation,
        // not merely an idle worker (which could also mean discarded work).
        var published = (try snapshot_cache.snapshot(alloc, "articles")) orelse return error.ExpectedActivationPublication;
        defer published.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), published.items.len);
        const status = published.items[0];
        try std.testing.expectEqual(@as(u64, 7001), status.group_id);
        try std.testing.expectEqual(generations.generation, status.metadata.lsm_root_generation);
        try std.testing.expectEqual(.fresh, status.metadata.freshness);
        try std.testing.expectEqual(.live_writer_publish, status.metadata.source);
        try std.testing.expect(status.metadata.target_observation_complete);
        try std.testing.expect(status.metadata.updated_at_ns > before_status.metadata.updated_at_ns);
        const activated = for (status.stats.indexes) |item| {
            if (std.mem.eql(u8, item.name, "dense_idx")) break item;
        } else return error.ExpectedActivatedDenseIndex;
        try std.testing.expectEqual(expected_index.coverage_generation, activated.coverage_generation);
        try std.testing.expectEqual(expected_index.coverage_config_hash, activated.coverage_config_hash);
    }
    const initial_cache_stats = owner_source.cacheStats();
    try std.testing.expect(initial_cache_stats.miss_count >= 1);
    const initial_context_metrics = try owner_source.contextMetrics();
    try std.testing.expectEqual(abi.abi_version, initial_context_metrics.version);
    const initial_maintenance = try owner_source.maintenanceSource().maintenanceSnapshot(false);
    try std.testing.expectEqual(@as(usize, 1), initial_maintenance.owner_count);
    _ = write_source.lsmMaintenanceScoreBestEffort();
    _ = write_source.nextLsmMaintenanceWakeDelayNsBestEffort();
    const initial_round = try owner_source.maintenanceSource().runLsmRound(false);
    if (initial_round.group_id) |group_id| try std.testing.expectEqual(@as(u64, 7001), group_id);

    _ = try write_source.source().batch(alloc, "articles", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"category\":\"news\",\"amount\":10,\"url\":\"data:text/plain;base64,YWxwaGEgYmV0YQ==\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"category\":\"news\",\"amount\":20,\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
        },
        .graph_writes = &.{.{
            .index_name = "relations_graph",
            .source = "doc:a",
            .target = "doc:b",
            .edge_type = "mentions",
            .weight = 0.75,
        }},
        .timestamp_ns = 4242,
        .sync_level = .full_index,
    });
    const txn_id: db_mod.types.TxnId = .{ 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
    const txn_participant = "table2:00000008:articles:7001";
    try std.testing.expect((try write_source.source().txnBeginGroupLocal(
        alloc,
        7001,
        "articles",
        txn_id,
        5_000,
        0,
        true,
        &.{txn_participant},
    )) != null);
    try std.testing.expectEqual(
        db_mod.types.TxnStatus.pending,
        (try write_source.source().txnStatusGroupLocal(alloc, 7001, "articles", txn_id)).?,
    );
    try std.testing.expect((try write_source.source().txnPrepareGroupLocal(
        alloc,
        7001,
        "articles",
        txn_id,
        0,
        .{ .writes = &.{.{
            .key = "doc:txn",
            .value = "{\"title\":\"transactional\",\"category\":\"news\",\"amount\":30}",
        }} },
    )) != null);
    try std.testing.expect((try write_source.source().txnResolveGroupLocal(
        alloc,
        7001,
        "articles",
        txn_id,
        .committed,
        5_001,
        0,
        .full_index,
    )) != null);
    try std.testing.expect((try write_source.source().txnAcknowledgeGroupLocal(
        alloc,
        7001,
        "articles",
        txn_id,
        txn_participant,
    )) != null);

    const backup_formats = [_]struct {
        format: backup_contract.BackupFormat,
        backup_id: []const u8,
    }{
        .{ .format = .portable, .backup_id = "portable-owner" },
        .{ .format = .native, .backup_id = "native-owner" },
    };
    var restore_shards: [backup_formats.len]?[]backup_contract.ShardSnapshot = @splat(null);
    defer for (&restore_shards) |*shards| {
        if (shards.*) |value| table_writes.freeStorageKernelBackupShards(alloc, value);
    };
    for (backup_formats, 0..) |backup, backup_index| {
        const shards = (try write_source.source().backupTable(alloc, "articles", .{
            .backup_root = backup_root,
            .backup_id = backup.backup_id,
            .format = backup.format,
        })).?;
        restore_shards[backup_index] = shards;
        try std.testing.expectEqual(@as(usize, 1), shards.len);
        try std.testing.expectEqual(@as(u64, 7001), shards[0].group_id);
        try std.testing.expect(shards[0].artifact_size_bytes > 0);
        try std.testing.expectEqual(@as(usize, 64), shards[0].artifact_sha256.len);
        const artifact_path = try std.fs.path.join(alloc, &.{ backup_root, shards[0].snapshot_path });
        defer alloc.free(artifact_path);
        var backup_io_impl = std.Io.Threaded.init(alloc, .{});
        defer backup_io_impl.deinit();
        _ = try std.Io.Dir.cwd().statFile(backup_io_impl.io(), artifact_path, .{});
    }
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expectEqual(
        db_mod.types.TxnStatus.committed,
        (try write_source.source().txnStatusGroupLocal(alloc, 7001, "articles", txn_id)).?,
    );
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expect((try write_source.source().beginBulkIngest(alloc, "articles")) != null);
    try std.testing.expect(write_source.hasActiveBulkIngestSession());
    try std.testing.expectEqual(
        @as(usize, 0),
        (try owner_source.maintenanceSource().maintenanceSnapshot(false)).owner_count,
    );
    try std.testing.expectEqual(
        @as(?u64, null),
        (try owner_source.maintenanceSource().runLsmRound(false)).group_id,
    );
    try std.testing.expect((try write_source.source().beginBulkIngest(alloc, "articles")) != null);
    _ = try write_source.source().batch(alloc, "articles", .{
        .writes = &.{.{
            .key = "doc:bulk",
            .value = "{\"title\":\"bulk\",\"category\":\"archive\",\"amount\":5}",
        }},
        .timestamp_ns = 4243,
        .sync_level = .write,
    });
    try std.testing.expect((try write_source.source().finishBulkIngest(
        alloc,
        "articles",
        .{ .compact = false },
    )) != null);
    try std.testing.expect(write_source.hasActiveBulkIngestSession());
    try std.testing.expect((try write_source.source().finishBulkIngest(
        alloc,
        "articles",
        .{ .compact = false },
    )) != null);
    try std.testing.expect(!write_source.hasActiveBulkIngestSession());
    try std.testing.expectEqual(
        @as(usize, 1),
        (try owner_source.maintenanceSource().maintenanceSnapshot(false)).owner_count,
    );
    _ = try write_source.runDensePostingMaintenanceRoundBestEffort();
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expect((try write_source.source().beginBulkIngest(alloc, "articles")) != null);
    try std.testing.expectError(
        error.AutoBulkIngestBusy,
        write_source.finishManagedWriterAutoBulkForTransition(7001, "articles"),
    );
    write_source.source().abortBulkIngest("articles");
    try write_source.finishManagedWriterAutoBulkForTransition(7001, "articles");
    try std.testing.expect(!write_source.finishExpiredAutoBulkIngestBestEffort());
    try std.testing.expectEqual(@as(u64, 0), write_source.autoBulkIngestStatsBestEffort().open_entries);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    const structural = (try owner_source.writeSource().reconcileTableGroupLocal(
        7001,
        "articles",
        null,
        false,
    )).?;
    try std.testing.expect(structural.state == .complete);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());

    var replicated_descriptor = try owner_source.loadDescriptor(alloc, 7001, "articles");
    defer replicated_descriptor.deinit(alloc);
    try owner_source.applyPreparedReplicatedBatchGroupLocal(
        alloc,
        7001,
        "articles",
        replicated_descriptor.view(),
        .{
            .timestamp_ns = 4243,
            .sync_level = .full_index,
        },
    );
    {
        // Prepared replay must remain usable while the catalog is unavailable,
        // and must fail closed if no resident generation was prepared. Use an
        // empty command: replicated deletes require separately ordered graph
        // cleanup, which this owner-sharing fixture does not drive.
        const saved_catalog = owner_source.catalog;
        owner_source.catalog = table_catalog.emptyCatalogSource();
        defer owner_source.catalog = saved_catalog;
        try std.testing.expect((try owner_source.writeSource().replicatedBatchGroupLocal(
            alloc,
            7001,
            "articles",
            .{},
            true,
            null,
        )) != null);
        try std.testing.expectError(error.RaftApplyWriterUnavailable, owner_source.writeSource().replicatedBatchGroupLocal(
            alloc,
            7002,
            "unprepared",
            .{},
            true,
            null,
        ));
    }
    try owner_source.waitForCurrentSyncGroupLocal(7001, "articles", .full_index);
    try owner_source.applyHotStandbyReplicationRecordGroupLocal(7001, "articles", .{
        .kind = .checkpoint,
        .cluster_id = 1,
        .shard_id = 7001,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 0,
        .previous_lsn = 0,
    });

    var lookup = (try read_source.source().lookup(alloc, "articles", "doc:a", .{
        .fields = &.{"title"},
        .include_all_fields = false,
    }, .read_index)).?;
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4242), lookup.version);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "dense_idx") == null);

    var txn_lookup = (try read_source.source().lookup(alloc, "articles", "doc:txn", .{}, .read_index)).?;
    defer txn_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, txn_lookup.json, "transactional") != null);

    try std.testing.expect((try read_source.source().lookup(
        alloc,
        "articles",
        "doc:missing",
        .{},
        .read_index,
    )) == null);

    var scan = (try read_source.source().scan(alloc, "articles", "doc:a", "doc:z", .{
        .inclusive_from = true,
        .exclusive_to = true,
        .include_documents = true,
        .limit = 10,
        .fields = &.{"title"},
        .include_all_fields = false,
        .filter_query_json = "{\"term\":{\"path\":\"/title\",\"value\":\"alpha\"}}",
    }, .read_index)).?;
    defer scan.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "alpha") != null);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "doc:b") == null);
    try std.testing.expect(std.mem.indexOf(u8, scan.ndjson, "dense_idx") == null);

    var query = try query_api.parsePublicQueryRequest(
        alloc,
        null,
        "articles",
        "{\"query\":{\"match_all\":{}},\"limit\":10}",
    );
    defer query.deinit(alloc);
    var response = (try read_source.source().query(alloc, "articles", query.req, .read_index)).?;
    defer response.deinit(alloc);

    try std.testing.expect(std.mem.indexOf(u8, response.json, "articles") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "doc:b") != null);

    var dense_query = try query_api.parseQueryRequest(
        alloc,
        null,
        "articles",
        "{\"embeddings\":{\"dense_idx\":[1.0,0.0,0.0]},\"indexes\":[\"dense_idx\"],\"limit\":2}",
    );
    defer dense_query.deinit(alloc);
    var preflight = (try read_source.source().preflightQuery(
        alloc,
        "articles",
        dense_query.req,
        .read_index,
        1000,
    )).?;
    defer preflight.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), preflight.dense_query_count);
    try std.testing.expectEqual(@as(usize, 1), preflight.embedding_indexes.len);
    try std.testing.expectEqualStrings("dense_idx", preflight.embedding_indexes[0].name);
    // Named composed embeddings are not the single-vector worker fast path.
    try std.testing.expectEqual(@as(u32, 0), preflight.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), preflight.vector_worker_fallback_count);

    var dense_response = (try read_source.source().query(alloc, "articles", dense_query.req, .read_index)).?;
    defer dense_response.deinit(alloc);
    const first_doc_a = std.mem.indexOf(u8, dense_response.json, "doc:a") orelse return error.MissingDenseHit;
    const second_doc_b = std.mem.indexOf(u8, dense_response.json, "doc:b") orelse return error.MissingDenseHit;
    try std.testing.expect(first_doc_a < second_doc_b);

    const text_stats_body =
        \\{"fields":[{"index_name":"full_text_index_v0","field":"_all","terms":["alpha"]}]}
    ;
    var text_stats_response = (try read_source.source().textStatsGroupLocal(
        alloc,
        7001,
        "articles",
        text_stats_body,
    )).?;
    defer text_stats_response.deinit(alloc);
    var text_stats = try table_reads.parseTextStatsHttpResponse(alloc, text_stats_body, text_stats_response.json);
    defer text_stats.deinit(alloc);
    switch (text_stats) {
        .fields => |fields| {
            try std.testing.expectEqual(@as(usize, 1), fields.fields.len);
            try std.testing.expectEqual(@as(u32, 4), fields.fields[0].global_doc_count);
            try std.testing.expectEqual(@as(u32, 1), fields.fields[0].term_doc_freqs[0].doc_freq);
        },
        .background_fields => return error.UnexpectedBackgroundTextStats,
    }
    {
        // This is the stable four-document cut. Later artifact reprocessing
        // accepts a new generation without waiting for child replacement, so
        // its live document count must not be compared with this old cut.
        var stable_statuses = (try write_source.source().localRuntimeStatuses(alloc, "articles")).?;
        defer stable_statuses.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), stable_statuses.items.len);
        try std.testing.expectEqual(@as(u64, 4), stable_statuses.items[0].stats.doc_count);
    }
    try std.testing.expectError(
        error.IdentityReadGenerationChanged,
        read_source.source().textStatsGroupLocal(
            alloc,
            7001,
            "articles",
            "{\"_identity_read_generation\":999999,\"fields\":[{\"index_name\":\"full_text_index_v0\",\"field\":\"_all\",\"terms\":[\"alpha\"]}]}",
        ),
    );

    const algebraic_ir = db_mod.algebraic.ir;
    const sum_expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "sum_by_category",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var sum_plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, sum_expr)).?;
    defer sum_plan.deinit(alloc);
    const algebraic_body = try table_reads.encodeAlgebraicPartialsRequestWithProgramAtGeneration(
        alloc,
        "alg",
        null,
        &.{sum_plan.access_path},
        &.{sum_expr},
        null,
    );
    defer alloc.free(algebraic_body);
    var algebraic_response = (try read_source.source().algebraicPartialsGroupLocal(
        alloc,
        7001,
        "articles",
        algebraic_body,
    )).?;
    defer algebraic_response.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, algebraic_response.json, "\"partials\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, algebraic_response.json, "sum_by_category") != null);
    try std.testing.expectError(
        error.InvalidQueryRequest,
        read_source.source().algebraicPartialsGroupLocal(alloc, 7001, "articles", "{}"),
    );

    var expand_req = distributed_graph.GraphExpandRequest{
        .name = try alloc.dupe(u8, "mentions"),
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .frontier = try alloc.dupe(distributed_graph.GraphFrontierItem, &.{.{
            .id = 1,
            .key = try alloc.dupe(u8, "doc:a"),
        }}),
        .exclude_nodes = @constCast((&[_]distributed_graph.GraphNodeIdentity{})[0..]),
        .exclude_edges = @constCast((&[_][]u8{})[0..]),
        .params = .{ .max_depth = 1, .max_results = 10 },
    };
    defer expand_req.deinit(alloc);
    var expand_response = (try read_source.source().graphExpandGroupLocal(
        alloc,
        7001,
        "articles",
        expand_req,
        .read_index,
    )).?;
    defer expand_response.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), expand_response.expansions.len);
    try std.testing.expectEqualStrings("doc:a", expand_response.expansions[0].frontier_key);
    try std.testing.expect(expand_response.expansions[0].graph_result.nodes.len > 0);

    var graph_lookup = (try indexes_api.lookupSingleIndexConfig(
        alloc,
        Catalog.indexes_json,
        "relations_graph",
    )).?;
    defer graph_lookup.deinit();
    const graph_identity = (try indexes_api.indexRuntimeIdentity(
        alloc,
        "relations_graph",
        graph_lookup.config,
    )).?;
    var hydrate_req = distributed_graph.GraphHydrateRequest{
        .keys = try alloc.alloc([]u8, 2),
        .incoming_index_name = try alloc.dupe(u8, "relations_graph"),
        .incoming_index_identity = .{
            .incarnation = graph_identity.incarnation,
            .config_hash = graph_identity.config_hash,
        },
        .incoming_index_name_owned = true,
    };
    hydrate_req.keys[0] = try alloc.dupe(u8, "doc:a");
    hydrate_req.keys[1] = try alloc.dupe(u8, "doc:b");
    defer hydrate_req.deinit(alloc);
    var hydrate_response = (try read_source.source().graphHydrateGroupLocal(
        alloc,
        7001,
        "articles",
        hydrate_req,
        .read_index,
    )).?;
    defer hydrate_response.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), hydrate_response.hits.len);
    try std.testing.expectEqualSlices(bool, &.{ false, true }, hydrate_response.has_incoming);
    try std.testing.expect(hydrate_response.incoming_index_identity.eql(hydrate_req.incoming_index_identity));

    const graph_path = db_mod.algebraic.ir.graphEdgeAccessPath("relations_graph");
    var edges_req = distributed_graph.GraphEdgesRequest{
        .index_name = try alloc.dupe(u8, "relations_graph"),
        .key = try alloc.dupe(u8, "doc:a"),
        .direction = .out,
        .tensor_access_path = .{
            .owner = try alloc.dupe(u8, graph_path.owner),
            .layout = graph_path.layout,
            .fragments = try alloc.dupe(db_mod.algebraic.ir.TensorFragment, graph_path.fragments),
            .output_dims = try alloc.dupe(db_mod.algebraic.ir.Dimension, graph_path.output_dims),
            .law_ids = try alloc.dupe(db_mod.algebraic.law.Id, graph_path.law_ids),
        },
        .tensor_program = try distributed_graph.graphEdgesTensorProgramEnvelopeAlloc(alloc, "relations_graph"),
    };
    defer edges_req.deinit(alloc);
    var edges_response = (try read_source.source().graphEdgesGroupLocal(
        alloc,
        7001,
        "articles",
        edges_req,
        .read_index,
    )).?;
    defer edges_response.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), edges_response.edges.len);
    try std.testing.expectEqualStrings("doc:b", edges_response.edges[0].target);
    try std.testing.expect(edges_response.scanned_rows >= edges_response.edges.len);

    var manifest = (try read_source.source().documentArtifactManifest(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
        .read_index,
    )) orelse return error.MissingDocumentArtifactManifest;
    defer manifest.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", manifest.document_id);
    try std.testing.expectEqualStrings("document_units_v1", manifest.artifact_name);
    try std.testing.expectEqualStrings("text", manifest.route_type);
    try std.testing.expectEqual(@as(usize, 1), manifest.unit_count);
    try std.testing.expect(manifest.child_ranges.len > 0);
    try std.testing.expectEqualStrings("unit", manifest.child_ranges[0].range_kind);
    try std.testing.expect(std.mem.indexOf(u8, manifest.manifest_json, "\"range_policy\"") != null);
    try std.testing.expect(manifest.state_json != null);

    var manifests = (try read_source.source().documentArtifactManifests(
        alloc,
        "articles",
        "doc:a",
        .read_index,
    )) orelse return error.MissingDocumentArtifactManifestList;
    defer manifests.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", manifests.document_id);
    try std.testing.expectEqual(@as(usize, 1), manifests.artifacts.len);
    try std.testing.expectEqualStrings("document_units_v1", manifests.artifacts[0].artifact_name);
    try std.testing.expect(manifests.artifacts[0].child_ranges.len > 0);
    try std.testing.expect(manifests.artifacts[0].state_json != null);
    try std.testing.expect((try read_source.source().documentArtifactManifest(
        alloc,
        "articles",
        "doc:missing",
        "document_units_v1",
        .read_index,
    )) == null);

    try std.testing.expectEqual(@as(?bool, true), try write_source.source().updateDocumentArtifactChildRangePlacement(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
        .{
            .range_id = "range:000000",
            .placement = "remote",
            .owner_group_id = 7002,
            .placement_generation = 3,
            .route_status = "remote_committed",
        },
    ));
    var moved_manifest = (try read_source.source().documentArtifactManifest(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
        .read_index,
    )) orelse return error.MissingDocumentArtifactManifest;
    defer moved_manifest.deinit(alloc);
    try std.testing.expectEqualStrings("remote", moved_manifest.child_ranges[0].placement);
    try std.testing.expectEqual(@as(?u64, 7002), moved_manifest.child_ranges[0].owner_group_id);

    const generation_before_reprocess = moved_manifest.generation;
    try std.testing.expectEqual(@as(?bool, true), try write_source.source().reprocessDocumentArtifact(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
    ));
    var reprocessed_manifest = (try read_source.source().documentArtifactManifest(
        alloc,
        "articles",
        "doc:a",
        "document_units_v1",
        .read_index,
    )) orelse return error.MissingDocumentArtifactManifest;
    defer reprocessed_manifest.deinit(alloc);
    try std.testing.expect(reprocessed_manifest.generation > generation_before_reprocess);

    var range_reprocess = (try write_source.source().reprocessDocumentArtifactRangeGroupLocal(
        alloc,
        7001,
        "articles",
        "document_units_v1",
        .{ .from_key = "doc:a", .to_key = "doc:b", .limit = 1 },
    )) orelse return error.StorageKernelArtifactOperationMissing;
    defer range_reprocess.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), range_reprocess.scanned);
    try std.testing.expectEqual(@as(usize, 1), range_reprocess.reprocessed);

    var repair_issues = (try write_source.source().listArtifactRepairIssuesGroupLocal(
        alloc,
        7001,
        "articles",
        .{ .limit = 10 },
    )) orelse return error.StorageKernelArtifactOperationMissing;
    defer repair_issues.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 10), repair_issues.limit);

    try std.testing.expectEqual(@as(?u64, 0), try write_source.source().applyDocumentArtifactChildRangeBatchGroupLocal(
        alloc,
        7001,
        "articles",
        "doc:a",
        "document_units_v1",
        .{},
    ));
    try std.testing.expect((try write_source.source().corruptEmbeddingArtifact(
        alloc,
        "articles",
        "doc:a",
        "dense_idx",
    )) != null);

    const CancelRepair = struct {
        fn requested(_: *anyopaque) bool {
            return true;
        }
    };
    var cancel_token: u8 = 0;
    try std.testing.expectError(error.Canceled, write_source.source().repairArtifactIssuesGroupLocalControlled(
        alloc,
        7001,
        "articles",
        .{ .limit = 10 },
        .{ .cancel_check = .{ .ptr = &cancel_token, .is_requested = CancelRepair.requested } },
    ));

    var cancellation = std.atomic.Value(bool).init(true);
    edges_req.cancellation = db_mod.types.CancellationToken.fromAtomic(&cancellation);
    try std.testing.expectError(
        error.Cancelled,
        read_source.source().graphEdgesGroupLocal(alloc, 7001, "articles", edges_req, .read_index),
    );
    cancellation.store(false, .release);
    edges_req.execution_deadline_ns = 1;
    try std.testing.expectError(
        error.Timeout,
        read_source.source().graphEdgesGroupLocal(alloc, 7001, "articles", edges_req, .read_index),
    );

    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expect(lease_capture.count > 0);
    try std.testing.expectEqual(@as(u64, 7001), lease_capture.last_group_id);
    try std.testing.expect(owner_source.cacheStats().hit_count > initial_cache_stats.hit_count);

    // Runtime status is a periodic observation. Bulk/replay publication may
    // still be sampling the preceding document set after the writes complete.
    const status_deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    var statuses = ready: while (true) {
        if (try write_source.source().localRuntimeStatuses(alloc, "articles")) |observed| {
            var current = observed;
            if (current.items.len == 1 and current.items[0].stats.doc_count == 4) break :ready current;
            current.deinit(alloc);
        }
        if (@import("antfly_platform").time.monotonicNs() >= status_deadline) return error.RuntimeStatusPublicationTimeout;
        try std.testing.io.sleep(.fromMilliseconds(5), .awake);
    };
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(.live_writer_publish, statuses.items[0].metadata.source);
    try std.testing.expectEqual(.fresh, statuses.items[0].metadata.freshness);
    try std.testing.expect(statuses.items[0].lsm_storage_stats != null);
    try std.testing.expect((try owner_source.restoreState(alloc, 7001, "articles")) == null);

    const lease_count_before_reopen = lease_capture.count;
    try std.testing.expectEqual(@as(usize, 1), owner_source.retireTable("articles"));
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    try std.testing.expect((try read_source.source().localRuntimeStatuses(alloc, "articles")) == null);
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    try std.testing.expectError(
        error.StorageReadTemporarilyUnavailable,
        read_source.source().observedDynamicFieldCapabilitySets(alloc, "articles", .{}),
    );
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    var reopened_lookup = (try read_source.source().lookup(
        alloc,
        "articles",
        "doc:b",
        .{},
        .read_index,
    )).?;
    defer reopened_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, reopened_lookup.json, "beta") != null);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    try std.testing.expect(lease_capture.count > lease_count_before_reopen);

    const restore_source_location = try std.fmt.allocPrint(alloc, "file://{s}", .{backup_root});
    defer alloc.free(restore_source_location);
    const PublicationHook = struct {
        publish_count: usize = 0,
        rollback_count: usize = 0,
        fail_publish: bool = false,

        fn iface(self: *@This()) backup_contract.RestorePublicationHook {
            return .{
                .ptr = self,
                .publish_definition = publish,
                .rollback_definition = rollback,
            };
        }

        fn publish(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.publish_count += 1;
            if (self.fail_publish) return error.TestRestoreDefinitionRejected;
        }

        fn rollback(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.rollback_count += 1;
        }
    };
    // Exercise the production maintenance/status paths while restore stages,
    // publishes, rejects metadata publication, reconciles, and reopens owners.
    // Unlike the deterministic held-lease regression, this uses native workers
    // and the real compiled storage archive and backup bytes.
    const RestoreMaintenance = struct {
        source: *kernel_owner_source.ProvisionedKernelOwnerSource,
        stop: std.atomic.Value(bool) = .init(false),
        rounds: std.atomic.Value(usize) = .init(0),
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            while (!self.stop.load(.acquire)) {
                self.round() catch |err| {
                    self.failure = err;
                    return;
                };
                _ = self.rounds.fetchAdd(1, .release);
                std.testing.io.sleep(.fromMilliseconds(1), .awake) catch return;
            }
        }

        fn round(self: *@This()) !void {
            const maintenance = self.source.maintenanceSource();
            _ = try maintenance.maintenanceSnapshot(false);
            _ = try maintenance.runLsmRound(false);
            maintenance.publishRuntimeStatuses();
        }
    };
    var concurrent_maintenance = RestoreMaintenance{ .source = &owner_source };
    const maintenance_thread = try std.Thread.spawn(.{}, RestoreMaintenance.run, .{&concurrent_maintenance});
    var maintenance_joined = false;
    defer if (!maintenance_joined) {
        concurrent_maintenance.stop.store(true, .release);
        maintenance_thread.join();
    };
    const maintenance_deadline = std.Io.Clock.awake.now(std.testing.io).nanoseconds + 5 * std.time.ns_per_s;
    while (concurrent_maintenance.rounds.load(.acquire) == 0) {
        if (std.Io.Clock.awake.now(std.testing.io).nanoseconds >= maintenance_deadline) return error.TestMaintenanceNotStarted;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const rounds_before = concurrent_maintenance.rounds.load(.acquire);
    for (backup_formats, 0..) |backup, backup_index| {
        const mutation_key = if (backup.format == .portable) "doc:after-portable" else "doc:after-native";
        _ = try write_source.source().batch(alloc, "articles", .{
            .writes = &.{.{
                .key = mutation_key,
                .value = "{\"title\":\"must disappear after restore\"}",
            }},
            .timestamp_ns = 6000 + @as(u64, @intCast(backup_index)),
            .sync_level = .full_index,
        });
        var mutation_lookup = (try read_source.source().lookup(
            alloc,
            "articles",
            mutation_key,
            .{},
            .read_index,
        )).?;
        mutation_lookup.deinit(alloc);

        const backup_manifest: backup_contract.TableBackupManifest = .{
            .format = backup.format,
            .backup_id = backup.backup_id,
            .table_name = "articles",
            .description = "storage owner restore composition",
            .schema_json = "",
            .read_schema_json = "",
            .indexes_json = Catalog.indexes_json,
            .replication_sources_json = "",
            .shards = restore_shards[backup_index].?,
        };
        const restore_plan: backup_contract.TableRestorePlan = .{
            .backup_root = backup_root,
            .manifest = &backup_manifest,
            .artifact_backup_id = backup.backup_id,
            .source_location = restore_source_location,
            .replace_existing = true,
        };
        if (backup_index == 0) {
            var cancelled = std.atomic.Value(bool).init(true);
            var cancelled_plan = restore_plan;
            cancelled_plan.cancellation = .fromAtomic(&cancelled);
            try std.testing.expectError(error.Canceled, write_source.source().restoreTable(alloc, "articles", cancelled_plan));
            var hook = PublicationHook{ .fail_publish = true };
            var rejected_plan = restore_plan;
            rejected_plan.publication_hook = hook.iface();
            try std.testing.expectError(
                error.TestRestoreDefinitionRejected,
                write_source.source().restoreTable(alloc, "articles", rejected_plan),
            );
            try std.testing.expectEqual(@as(usize, 1), hook.publish_count);
            try std.testing.expectEqual(@as(usize, 0), hook.rollback_count);
            var retained_lookup = (try read_source.source().lookup(
                alloc,
                "articles",
                mutation_key,
                .{},
                .read_index,
            )).?;
            retained_lookup.deinit(alloc);
        }
        try std.testing.expect((try write_source.source().restoreTable(
            alloc,
            "articles",
            restore_plan,
        )) != null);
        try std.testing.expect((try read_source.source().lookup(
            alloc,
            "articles",
            mutation_key,
            .{},
            .read_index,
        )) == null);
        var restored_lookup = (try read_source.source().lookup(
            alloc,
            "articles",
            "doc:a",
            .{},
            .read_index,
        )).?;
        defer restored_lookup.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, restored_lookup.json, "alpha") != null);
        try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());

        var restore_state = (try owner_source.restoreState(alloc, 7001, "articles")) orelse
            return error.TestExpectedRestoreState;
        defer restore_state.deinit(alloc);
        try std.testing.expectEqualStrings(backup.backup_id, restore_state.backup_id);
        try std.testing.expectEqualStrings(restore_source_location, restore_state.location);
        try std.testing.expectEqualStrings(
            restore_shards[backup_index].?[0].artifact_sha256,
            restore_state.artifact_sha256,
        );
        try std.testing.expectEqual(@as(u64, 7001), restore_state.group_id);
        try std.testing.expect(restore_state.primary_restored);
        try std.testing.expect(restore_state.runtime_repair_complete);

        // An exact retry repairs through the same resident owner, and a
        // reconcile-only pass validates the committed physical identity.
        try std.testing.expect((try write_source.source().restoreTable(
            alloc,
            "articles",
            restore_plan,
        )) != null);
        var reconcile_plan = restore_plan;
        reconcile_plan.reconcile_only = true;
        reconcile_plan.replace_existing = false;
        try std.testing.expect((try write_source.source().restoreTable(
            alloc,
            "articles",
            reconcile_plan,
        )) != null);
        try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    }

    concurrent_maintenance.stop.store(true, .release);
    maintenance_thread.join();
    maintenance_joined = true;
    if (concurrent_maintenance.failure) |err| return err;
    try std.testing.expect(concurrent_maintenance.rounds.load(.acquire) > rounds_before);

    const accepted_snapshot = try shard_state_store.encodeGroupStateSnapshot(
        alloc,
        .{ .start = "", .end = "" },
        &.{.{
            .key = "doc:snapshot",
            .value = "{\"title\":\"published snapshot\",\"category\":\"archive\",\"amount\":30}",
        }},
        &.{},
    );
    defer alloc.free(accepted_snapshot);
    try write_source.installRaftSnapshotGroupLocal(alloc, 7001, accepted_snapshot);
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    try std.testing.expectEqual(@as(u64, 1), generations.generation);
    try std.testing.expectEqual(@as(usize, 0), generations.reservations);
    try std.testing.expect((try read_source.source().lookup(
        alloc,
        "articles",
        "doc:b",
        .{},
        .read_index,
    )) == null);
    var published_snapshot_lookup = (try read_source.source().lookup(
        alloc,
        "articles",
        "doc:snapshot",
        .{},
        .read_index,
    )).?;
    defer published_snapshot_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, published_snapshot_lookup.json, "published snapshot") != null);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());

    const rejected_snapshot = try shard_state_store.encodeGroupStateSnapshot(
        alloc,
        .{ .start = "", .end = "" },
        &.{.{ .key = "doc:rejected", .value = "{\"title\":\"rejected snapshot\"}" }},
        &.{},
    );
    defer alloc.free(rejected_snapshot);
    catalog.accept_publication = false;
    try std.testing.expectError(
        error.CatalogChanged,
        write_source.installRaftSnapshotGroupLocal(alloc, 7001, rejected_snapshot),
    );
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    try std.testing.expectEqual(@as(u64, 1), generations.generation);
    try std.testing.expectEqual(@as(usize, 0), generations.reservations);
    try std.testing.expect((try read_source.source().lookup(
        alloc,
        "articles",
        "doc:rejected",
        .{},
        .read_index,
    )) == null);
    var rolled_back_snapshot_lookup = (try read_source.source().lookup(
        alloc,
        "articles",
        "doc:snapshot",
        .{},
        .read_index,
    )).?;
    defer rolled_back_snapshot_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, rolled_back_snapshot_lookup.json, "published snapshot") != null);
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());

    const group_path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root});
    defer alloc.free(group_path);
    const open_request: abi.OpenRequest = .{
        .path = .fromSlice(group_path),
        .table_name = .fromSlice("articles"),
        .has_identity_namespace = 1,
        .identity_table_id = 7,
        .identity_shard_id = 7001,
        .identity_range_id = 7001,
    };
    var duplicate: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.lsm_root_writer_already_open, abi.antfly_storage_owner_open(&open_request, &duplicate));
    try std.testing.expect(duplicate == null);

    const cleanup_contract = @import("../metadata/topology_protocol.zig").DropCleanupContract{
        .table_id = 7,
        .expected_transition_generation = 0,
        .group_ids = &.{7001},
    };
    try std.testing.expectError(error.DropCleanupOwnershipInconclusive, write_source.source().dropTable(alloc, "articles", cleanup_contract));
    try std.testing.expectEqual(@as(usize, 1), owner_source.ownerCountForTest());
    // Physical cleanup follows the committed catalog drop; it cannot retire
    // a group while a linearizable catalog view still assigns that owner.
    catalog.present = false;
    catalog.accept_publication = true;
    try std.testing.expect((try write_source.source().dropTable(alloc, "articles", cleanup_contract)) != null);
    try std.testing.expectEqual(@as(usize, 0), owner_source.ownerCountForTest());
    var path_io_impl = std.Io.Threaded.init(alloc, .{});
    defer path_io_impl.deinit();
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().access(path_io_impl.io(), group_path, .{}),
    );

    write_source.deinit();
    write_source_active = false;
    owner_source.deinit();
    owner_source_active = false;

    var reopened: ?*anyopaque = null;
    try std.testing.expectEqual(abi.Status.ok, abi.antfly_storage_owner_open(&open_request, &reopened));
    try std.testing.expect(reopened != null);
    abi.antfly_storage_owner_close(reopened);
}
