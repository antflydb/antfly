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

//! Transport-independent system catalog admission, capability fencing, and
//! exact receipt completion. Logical and physical table identities are separate.
const std = @import("std");
const domain = @import("domain.zig");
const storage = @import("../metadata/storage/raft_apply_store.zig");
const protocol = @import("../metadata/topology_protocol.zig");
const operation = @import("../api/operation.zig");
const tables_api = @import("../api/tables.zig");
const indexes_api = @import("../api/indexes.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const table_manager = @import("../metadata/table_manager.zig");
const settings = @import("settings.zig");

pub const Request = domain.Request;

pub fn mutate(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: Request) ![]u8 {
    try context.ensureActive();
    if (request.mutation.table_id != 0 or request.mutation.storage_name.len != 0) return error.InvalidCatalogMutation;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.system_catalog_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const admission = try store.systemCatalogAdmission(a, svc.metadata_group_id, request.mutation);
    var command: storage.SystemCatalogCommand = .{ .expected_revision = admission.meta.revision, .mutation = request.mutation };
    if (request.mutation.action == .create and request.mutation.kind == .table) {
        const json = request.create_table_json orelse return error.InvalidCatalogMutation;
        var req = try tables_api.parseStoredCreateTableRequest(a, json);
        const storage_name = request.physical_name orelse try std.fmt.allocPrint(a, "table:{d}", .{admission.meta.next_id});
        if (!std.mem.startsWith(u8, storage_name, "table:") or storage_name.len > 1024) return error.InvalidCatalogMutation;
        req.indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(a, storage_name, req.indexes_json orelse tables_api.default_indexes_json, tables_api.effectiveSchemaJson(req.schema_json));
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(a, req.indexes_json.?);
        try managed_embedder.validateEmbeddingProducerOwnershipJson(a, req.indexes_json.?);
        var table = tables_api.deriveTableRecord(storage_name, req);
        if (try fk_publication.schemaHasForeignKeys(a, table.schema_json)) return error.ForeignKeyGenerationPublicationRequired;
        const policy = admission.placement_policy;
        try policy.validate();
        if (policy.placement_role) |role| table.placement_role = role;
        if (policy.desired_replica_count) |count| table.desired_replica_count = count;
        if (req.num_shards == null) if (policy.min_ranges) |count| {
            table.min_ranges = count;
        };
        const generation = try svc.captureTableCreateGeneration(a, table.table_id);
        const ranges = try tables_api.deriveInitialRangesForGeneration(a, table, generation);
        command.mutation.table_id = table.table_id;
        command.mutation.storage_name = storage_name;
        command.topology = .{ .create = .{ .expected_transition_generation = generation, .table = table, .ranges = ranges } };
    } else if (request.create_table_json != null) return error.InvalidCatalogMutation;
    if (request.mutation.action == .set_tablespace and request.mutation.kind == .table) {
        const target: domain.Target = .{ .database = request.mutation.database, .namespace = request.mutation.namespace, .table = request.mutation.name };
        const current = (try store.resolveSystemCatalogTable(a, svc.metadata_group_id, target)) orelse return error.TableNotFound;
        const policy = admission.placement_policy;
        var replacement = current;
        replacement.placement_role = policy.placement_role orelse "data";
        replacement.desired_replica_count = policy.desired_replica_count orelse 3;
        replacement.min_ranges = policy.min_ranges orelse 1;
        command.placement_update = .{ .expected = current, .replacement = replacement };
    }
    const result = try store.prepareSystemCatalogResult(alloc, svc.metadata_group_id, command);
    errdefer alloc.free(result);
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_system_catalog = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash)) return error.MetadataMutationOutcomeUnknown;
    if (command.topology) |topology| svc.verifyTableCreateProjection(a, topology.create.table, topology.create.ranges) catch return error.MetadataMutationOutcomeUnknown;
    return result;
}

pub fn snapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext) ![]u8 {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var snapshot = try store.systemCatalogSnapshot(alloc, svc.metadata_group_id);
    defer snapshot.deinit();
    return std.json.Stringify.valueAlloc(alloc, snapshot.value, .{});
}

pub fn settingSnapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, scope: settings.Scope) ![]u8 {
    try context.ensureActive();
    if (scope.principal.len == 0 or scope.database.len == 0) return error.InvalidSettingRecord;
    // Native callers carry the authenticated principal. The metadata HTTP
    // transport drops that identity, but authenticates the internal service;
    // public API ingress must derive Scope, never accept it from SQL text.
    if (context.principal != null) {
        const admitted = context.setting_read_principal orelse return error.Forbidden;
        if (!std.mem.eql(u8, admitted, scope.principal)) return error.Forbidden;
    }
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlSettingSnapshotJson(alloc, svc.metadata_group_id, scope);
}

pub fn policySnapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("policies.zig").SnapshotRequest) ![]u8 {
    try context.ensureActive();
    if (request.table_id == 0 or request.principal.len == 0 or request.database.len == 0) return error.InvalidRowPolicyRecord;
    // This principal-independent catalog read cannot assert roles. The API
    // separately mints a short-lived authenticated role proof, which the
    // owner verifies against this immutable serving generation.
    if (request.roles.len != 0) return error.RowPolicyAuthenticationRequired;
    if (context.principal != null) {
        const admitted = context.setting_read_principal orelse return error.Forbidden;
        if (!std.mem.eql(u8, admitted, request.principal)) return error.Forbidden;
    }
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlPolicySnapshotJson(alloc, svc.metadata_group_id, request.table_id, request.principal, request.database, request.roles);
}

pub fn policyInstallSnapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("policies.zig").InstallRequest) ![]u8 {
    try context.ensureActive();
    if (!context.row_policy_install_authority or request.table_id == 0 or
        request.expected_generation == 0 or request.expected_catalog_epoch == 0)
        return error.Forbidden;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlPolicyInstallSnapshotJson(alloc, svc.metadata_group_id, request);
}

pub fn policyPublicationStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, table_id: u64) ![]u8 {
    try context.ensureActive();
    if (!context.row_policy_install_authority or table_id == 0) return error.Forbidden;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlPolicyPublicationStatusJson(alloc, svc.metadata_group_id, table_id);
}

pub fn policyPublicationWorkJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, after_table_id: u64) ![]u8 {
    try context.ensureActive();
    if (!context.row_policy_install_authority) return error.Forbidden;
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.sqlPolicyPublicationWorkJson(alloc, svc.metadata_group_id, after_table_id);
}

pub fn beginPolicyPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: @import("policies.zig").BeginRequest) ![]u8 {
    if (!context.setting_admin or !context.row_policy_install_authority or request.table_id == 0) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try store.sqlPolicyBeginCommandJson(alloc, svc.metadata_group_id, request);
    defer alloc.free(bytes);
    var command = try std.json.parseFromSlice(@import("policies.zig").PublicationCommand, alloc, bytes, .{});
    defer command.deinit();
    return mutatePolicyPublication(svc, alloc, context, command.value);
}

/// The reconciler supplies a precomputed owner cut and durable owner receipt;
/// metadata apply repeats the topology/ACK checks before committing. An
/// ambiguous proposal is reconciled by reading publication status, not by
/// blindly resubmitting a transition.
pub fn mutatePolicyPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: @import("policies.zig").PublicationCommand) ![]u8 {
    if (!context.setting_admin or !context.row_policy_install_authority) return error.Forbidden;
    try context.ensureActive();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_row_policy_publication_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const current = try store.systemCatalogMeta(alloc, svc.metadata_group_id);
    if (current.revision != command.expected_revision) return error.RowPolicyCatalogChanged;
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_sql_policy_publication = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash))
        return error.MetadataMutationOutcomeUnknown;
    return std.json.Stringify.valueAlloc(alloc, observed, .{});
}

fn serializePolicyDefinitionCommand(alloc: std.mem.Allocator, command: @import("policies.zig").Command) ![]u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    defer output.deinit();
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    try @import("../storage/db/relational_integrity_json.zig").write(command, &stream);
    return output.toOwnedSlice();
}

test "policy definition command serializes logical JSON literals without map pointers" {
    const alloc = std.testing.allocator;
    var literal = try std.json.parseFromSlice(std.json.Value, alloc, "{\"roles\":[\"reader\"],\"enabled\":true}", .{});
    defer literal.deinit();
    const command: @import("policies.zig").Command = .{
        .expected_revision = 1,
        .change = .{ .put = .{
            .id = 1,
            .generation = 1,
            .table_id = 2,
            .schema_version = 1,
            .schema_digest = @splat(1),
            .name = "p",
            .commands = .{ .select = true },
            .roles = &.{"reader"},
            .using = .{ .instructions = &.{.{ .type = .{ .kind = .boolean }, .operation = .{ .literal = literal.value } }}, .root = 0 },
        } },
    };
    const bytes = try serializePolicyDefinitionCommand(alloc, command);
    defer alloc.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "\"enabled\":true") != null);
    var decoded = try std.json.parseFromSlice(@import("policies.zig").Command, alloc, bytes, .{});
    defer decoded.deinit();
    try std.testing.expect(decoded.value.change.put.using.?.instructions[0].operation.literal.object.get("enabled").?.bool);
}

const fk_publication = @import("../metadata/fk_generation_publication.zig");

pub fn fkGenerationPublicationStatusJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, child_table_id: u64) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.fkGenerationPublicationStatusJson(alloc, svc.metadata_group_id, child_table_id);
}

pub fn fkGenerationPublicationWorkJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, after_child_table_id: u64) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.fkGenerationPublicationWorkJson(alloc, svc.metadata_group_id, after_child_table_id);
}

pub fn fkGenerationPublicationDecisionJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: fk_publication.DecisionRequest) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.fkGenerationPublicationDecisionJson(alloc, svc.metadata_group_id, request);
}

pub fn fkGenerationPublicationSourceDecisionJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: fk_publication.SourceDecisionRequest) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.fkGenerationPublicationSourceDecisionJson(alloc, svc.metadata_group_id, request);
}

pub fn mutateFkGenerationPublication(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: fk_publication.Command) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try command.validateShape();
    try context.ensureActive();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.fk_generation_publication_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > fk_publication.max_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_fk_generation_publication = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.fkGenerationPublicationStatusJson(alloc, svc.metadata_group_id, command.child_table_id) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(fk_publication.Publication, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    if (!std.mem.eql(u8, &parsed.value.plan.id, &command.plan_id) or parsed.value.revision != command.expected_revision + 1)
        return error.MetadataMutationOutcomeUnknown;
    return observed;
}

pub fn mutateFkInitialCreate(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: fk_publication.InitialCommand) ![]u8 {
    if (!context.fk_generation_publication_authority) return error.Forbidden;
    try command.validateShape();
    try context.ensureActive();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.fk_initial_create_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > fk_publication.max_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_fk_initial_create = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.fkInitialCreateStatusJson(alloc, svc.metadata_group_id, command.child_table_id) catch return error.MetadataMutationOutcomeUnknown;
    errdefer alloc.free(observed);
    var parsed = std.json.parseFromSlice(fk_publication.InitialPublication, alloc, observed, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    if (!std.mem.eql(u8, &parsed.value.plan.id, &command.plan_id) or parsed.value.revision != command.expected_revision + 1)
        return error.MetadataMutationOutcomeUnknown;
    return observed;
}

/// Durable draft-only policy mutation. The authorization is a body-bound
/// administrator service grant; this transition never installs an owner
/// bundle or flips the serving publication. Ambiguous outcomes require a
/// status read rather than replaying an uncertain command.
pub fn mutatePolicyDefinition(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, command: @import("policies.zig").Command) ![]u8 {
    if (!context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_row_policy_publication_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const current = try store.systemCatalogMeta(alloc, svc.metadata_group_id);
    if (current.revision != command.expected_revision) return error.RowPolicyCatalogChanged;
    const bytes = try serializePolicyDefinitionCommand(alloc, command);
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_sql_policies = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash))
        return error.MetadataMutationOutcomeUnknown;
    return std.json.Stringify.valueAlloc(alloc, observed, .{});
}

pub fn mutateSetting(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: settings.Request) ![]u8 {
    if (!context.setting_admin) return error.Forbidden;
    try context.ensureActive();
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.sql_setting_catalog_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var snapshot = try store.systemCatalogSnapshot(alloc, svc.metadata_group_id);
    defer snapshot.deinit();
    const state = snapshot.value;
    var command: settings.Command = .{ .expected_revision = state.revision, .change = undefined };
    switch (request) {
        .put => |input| {
            const prior = for (state.settings) |record| {
                if (std.ascii.eqlIgnoreCase(record.name, input.name)) break record;
            } else null;
            if (prior) |record| if (input.matches(record)) return std.json.Stringify.valueAlloc(alloc, snapshot.meta, .{});
            const identity: settings.Identity = if (prior) |record|
                .{ .id = record.identity.id, .generation = try std.math.add(u64, record.identity.generation, 1) }
            else
                .{ .id = state.next_id, .generation = 1 };
            const record = input.record(identity);
            try record.validate();
            command.change = .{ .put = record };
        },
        .drop => |name| {
            try settings.validateName(name);
            const prior = for (state.settings) |record| {
                if (std.ascii.eqlIgnoreCase(record.name, name)) break record;
            } else return std.json.Stringify.valueAlloc(alloc, snapshot.meta, .{});
            command.change = .{ .drop = prior.identity };
        },
    }
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_sql_settings = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash)) return error.MetadataMutationOutcomeUnknown;
    return std.json.Stringify.valueAlloc(alloc, observed, .{});
}

pub fn resolve(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, target: domain.Target) !?table_manager.TableRecord {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target);
}

fn listTablesJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: domain.TableList) ![]u8 {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const result = try store.listSystemCatalogTables(alloc, svc.metadata_group_id, request);
    errdefer alloc.free(result);
    try context.ensureActive();
    return result;
}

pub fn call(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, input: domain.Call) ![]u8 {
    return switch (input) {
        .setting_snapshot => |scope| settingSnapshotJson(svc, alloc, context, scope),
        .policy_snapshot => |request| policySnapshotJson(svc, alloc, context, request),
        .policy_install_snapshot => |request| policyInstallSnapshotJson(svc, alloc, context, request),
        .policy_publication_status => |table_id| policyPublicationStatusJson(svc, alloc, context, table_id),
        .policy_publication_work => |after_table_id| policyPublicationWorkJson(svc, alloc, context, after_table_id),
        .policy_publication_begin => |request| beginPolicyPublication(svc, alloc, context, request),
        .policy_definition_mutate => |command| mutatePolicyDefinition(svc, alloc, context, command),
        .policy_publication_mutate => |command| mutatePolicyPublication(svc, alloc, context, command),
        .fk_generation_publication_begin => |plan| mutateFkGenerationPublication(svc, alloc, context, .{ .plan_id = plan.id, .child_table_id = plan.child_before.table_id, .expected_revision = 0, .action = .begin, .plan = plan }),
        .fk_generation_publication_mutate => |command| mutateFkGenerationPublication(svc, alloc, context, command),
        .fk_generation_publication_status => |child_table_id| fkGenerationPublicationStatusJson(svc, alloc, context, child_table_id),
        .fk_generation_publication_work => |after_child_table_id| fkGenerationPublicationWorkJson(svc, alloc, context, after_child_table_id),
        .fk_generation_publication_decision => |request| fkGenerationPublicationDecisionJson(svc, alloc, context, request),
        .fk_generation_publication_source_decision => |request| fkGenerationPublicationSourceDecisionJson(svc, alloc, context, request),
        .fk_initial_create_prepare => |request| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialCreatePrepareJson(alloc, svc.metadata_group_id, request);
        },
        .fk_initial_child_decision => |request| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialChildDecisionJson(alloc, svc.metadata_group_id, request);
        },
        .fk_initial_create_begin => |plan| mutateFkInitialCreate(svc, alloc, context, .{ .plan_id = plan.id, .child_table_id = plan.child.table_id, .expected_revision = 0, .action = .begin, .plan = plan }),
        .fk_initial_create_mutate => |command| mutateFkInitialCreate(svc, alloc, context, command),
        .fk_initial_create_status => |child_table_id| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialCreateStatusJson(alloc, svc.metadata_group_id, child_table_id);
        },
        .fk_initial_create_work => |after_child_table_id| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialCreateWorkJson(alloc, svc.metadata_group_id, after_child_table_id);
        },
        .fk_initial_parent_decision => |request| blk: {
            if (!context.fk_generation_publication_authority) return error.Forbidden;
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.fkInitialParentDecisionJson(alloc, svc.metadata_group_id, request);
        },
        .setting_mutate => |request| mutateSetting(svc, alloc, context, request),
        .write_validation_revision => blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk std.json.Stringify.valueAlloc(alloc, @import("../metadata/api.zig").MetadataHead{
                .metadata_group_id = svc.metadata_group_id,
                .metadata_incarnation = try svc.metadataIncarnation(),
                .metadata_epoch = try store.writeValidationRevision(svc.metadata_group_id),
            }, .{});
        },
        .write_validation => |name| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const result = try store.tableWriteValidation(alloc, svc.metadata_group_id, name);
            errdefer alloc.free(result);
            try context.ensureActive();
            break :blk result;
        },
        .table_status => |target| listTablesJson(svc, alloc, context, target.listing()),
        .list_tables => |request| listTablesJson(svc, alloc, context, request),
        .export_snapshot => blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.exportSystemCatalog(alloc, svc.metadata_group_id);
        },
        .read => |request| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.systemCatalogRead(alloc, svc.metadata_group_id, request);
        },
        .query_definition => |name| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const definition = try store.queryTableDefinition(alloc, svc.metadata_group_id, name);
            defer if (definition) |value| value.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, definition, .{});
        },
        .snapshot => snapshotJson(svc, alloc, context),
        .resolve => |target| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const table = try store.resolveSystemCatalogIdentity(alloc, svc.metadata_group_id, target);
            defer if (table) |value| value.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, table, .{});
        },
        .resolve_many => |request| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const result = try store.resolveSystemCatalogIdentities(alloc, svc.metadata_group_id, request);
            defer result.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .mutate => |request| mutate(svc, alloc, context, request),
    };
}

/// Publish the restored physical incarnation and its logical binding together.
/// Job retries are accepted only when both projections match exactly.
pub fn restore(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, target: domain.Target, table: table_manager.TableRecord, source_ranges: []const table_manager.RangeRecord) !void {
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.system_catalog_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const meta = try store.systemCatalogMeta(alloc, svc.metadata_group_id);
    const admission = try svc.captureTableRestoreAdmission(alloc, table);
    const ranges = try @import("../metadata/table_topology_mutations.zig").deriveRestoreDestinationRanges(alloc, table, source_ranges, admission.incarnation_generation);
    defer {
        for (ranges) |range| table_manager.freeRange(alloc, range);
        alloc.free(ranges);
    }
    if (admission.already_applied) {
        const bound = (try store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target)) orelse return error.MetadataMutationOutcomeUnknown;
        defer table_manager.freeTable(alloc, bound);
        if (bound.table_id != table.table_id) return error.TableAlreadyExists;
        return svc.verifyTableCreateProjection(alloc, table, ranges);
    }
    const command: storage.SystemCatalogCommand = .{
        .expected_revision = meta.revision,
        .mutation = .{ .action = .create, .kind = .table, .database = target.database, .namespace = target.namespace, .name = target.table, .table_id = table.table_id, .storage_name = table.name },
        .topology = .{ .create = .{ .expected_transition_generation = admission.expected_transition_generation, .table = table, .ranges = ranges } },
    };
    try store.validateSystemCatalog(svc.metadata_group_id, command);
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_system_catalog = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const bound = (store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target) catch return error.MetadataMutationOutcomeUnknown) orelse return error.MetadataMutationOutcomeUnknown;
    defer table_manager.freeTable(alloc, bound);
    if (bound.table_id != table.table_id) return error.MetadataMutationOutcomeUnknown;
    svc.verifyTableCreateProjection(alloc, table, ranges) catch return error.MetadataMutationOutcomeUnknown;
}
