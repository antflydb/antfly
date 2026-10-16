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

//! Server catalog command envelope and admission policy.
const std = @import("std");
const domain = @import("antfly_local_sources").system_catalog_domain;
const Read = domain.Read;
const Request = domain.Request;
const ResolveMany = domain.ResolveMany;
const TableList = domain.TableList;
const TableStatusTarget = domain.TableStatusTarget;
const Target = domain.Target;

/// Internal, body-bound admission; never deserialize this from public SQL.
pub const RelationReplacement = struct {
    guard: domain.RelationMutationGuard,
    expected: @import("../metadata/table_manager.zig").TableRecord,
    replacement: @import("../metadata/table_manager.zig").TableRecord,

    pub fn validate(self: @This()) !void {
        try self.guard.validate();
        if (self.expected.table_id != self.guard.owner.table_id or self.replacement.table_id != self.expected.table_id or
            !std.mem.eql(u8, self.expected.name, self.replacement.name)) return error.InvalidCatalogMutation;
    }
};

pub const RelationReplacementResult = struct {
    schema_version: u32,
    stamp: ?@import("../metadata/api.zig").CatalogMutationStamp = null,
};

/// Metadata owns the full predecessor record; ingress supplies logical intent
/// and the owner it authorized, never a synthesized partial table record.
pub const RelationSchemaMutation = struct {
    guard: domain.RelationMutationGuard,
    schema_json: []const u8,

    pub fn validate(self: @This()) !void {
        try self.guard.validate();
        if (self.schema_json.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    }
};

pub const Call = union(enum) {
    lake_index_lifecycle_read: u64,
    lake_index_lifecycle_work: ?u64,
    lake_index_lifecycle_mutate: @import("../metadata/lake_index_lifecycle.zig").Write,
    setting_snapshot: @import("antfly_local_sources").system_catalog_settings.Scope,
    policy_snapshot: @import("antfly_local_sources").system_catalog_policies.SnapshotRequest,
    policy_install_snapshot: @import("antfly_local_sources").system_catalog_policies.InstallRequest,
    /// Returns a PublicationStamp, or JSON null when no policy was provisioned.
    /// Unsupported capabilities and inconsistent publications remain errors.
    policy_publication_status: u64,
    /// Narrow, linearizable supervisor work queue; excludes policy definitions.
    policy_publication_work: u64,
    /// Trusted metadata ingress derives the owner cut; callers supply no
    /// owner descriptors or policy bundle bytes.
    policy_publication_begin: @import("antfly_local_sources").system_catalog_policies.BeginRequest,
    /// Administrator-authored draft definition. Never activates enforcement.
    policy_definition_mutate: @import("antfly_local_sources").system_catalog_policies.Command,
    /// Private coordinator-only transition. Never accepted from public SQL.
    policy_publication_mutate: @import("antfly_local_sources").system_catalog_policies.PublicationCommand,
    fk_generation_publication_begin: @import("../metadata/fk_generation_publication.zig").Plan,
    fk_generation_publication_mutate: @import("../metadata/fk_generation_publication.zig").Command,
    fk_generation_publication_status: u64,
    fk_generation_publication_work: u64,
    fk_generation_publication_decision: @import("../metadata/fk_generation_publication.zig").DecisionRequest,
    fk_generation_publication_source_decision: @import("../metadata/fk_generation_publication.zig").SourceDecisionRequest,
    relational_index_begin: @import("../metadata/relational_index_publication.zig").BeginRequest,
    relational_index_admission_status: @import("../metadata/relational_index_publication.zig").AdmissionRequest,
    relational_index_owner_decision: @import("../metadata/relational_index_publication.zig").OwnerRequest,
    relational_index_owner_ack: @import("../metadata/relational_index_publication.zig").Ack,
    relational_index_owner_ack_status: @import("../metadata/relational_index_publication.zig").Ack,
    relational_index_publish_decide: @import("../metadata/relational_index_publication.zig").DecidePublish,
    relational_index_publish_status: @import("../metadata/relational_index_publication.zig").DecidePublish,
    relational_index_cancel: @import("../metadata/relational_index_publication.zig").CancelDecision,
    relational_index_cancel_status: @import("../metadata/relational_index_publication.zig").CancelDecision,
    relational_index_source_accept: @import("../metadata/relational_index_delivery_journal.zig").Request,
    relational_index_source_accept_status: @import("../metadata/relational_index_delivery_journal.zig").Query,
    relational_index_source_complete: @import("../metadata/relational_index_delivery_journal.zig").CompletionRequest,
    fk_initial_create_prepare: @import("../metadata/fk_generation_publication.zig").InitialCreatePrepareRequest,
    constraint_name_prepare: domain.ConstraintNamePrepareRequest,
    constraint_name_check: domain.ConstraintNameCheck,
    fk_initial_child_decision: @import("../metadata/fk_generation_publication.zig").InitialChildDecisionRequest,
    fk_initial_create_begin: @import("../metadata/fk_generation_publication.zig").InitialCreatePlan,
    fk_initial_create_mutate: @import("../metadata/fk_generation_publication.zig").InitialCommand,
    fk_initial_create_status: u64,
    fk_generation_table_locked: u64,
    fk_initial_create_work: u64,
    fk_initial_parent_decision: @import("../metadata/fk_generation_publication.zig").DecisionRequest,
    /// Internal, read-only discovery. A ticket is not authority to unlink.
    fk_initial_retirement_page: @import("../metadata/fk_initial_retirement_wire.zig").PageRequest,
    store_root_enroll: @import("../metadata/store_root_enrollment.zig").Request,
    /// Exact, linearizable read used to resolve an ambiguous enrollment response.
    store_root_enrollment_status: @import("../metadata/store_root_enrollment.zig").Identity,
    fk_initial_retirement_signed_page: @import("../metadata/fk_initial_retirement_wire.zig").SignedPageRequest,
    fk_initial_retirement_ack: @import("../metadata/fk_initial_retirement_wire.zig").AckRequest,
    setting_mutate: @import("antfly_local_sources").system_catalog_settings.Request,
    list_tables: TableList,
    export_snapshot: void,
    read: Read,
    snapshot: void,
    resolve: Target,
    resolve_many: ResolveMany,
    relation_replace: RelationReplacement,
    relation_schema_mutate: RelationSchemaMutation,
    query_definition: []const u8,
    mutate: Request,
    // A distinct operation makes older peers reject unsupported point reads.
    table_status: TableStatusTarget,
    write_validation: []const u8,
    write_validation_revision: void,

    pub fn requiresAdministrativeGrant(self: @This()) bool {
        return switch (self) {
            .relation_replace, .relation_schema_mutate, .constraint_name_prepare, .constraint_name_check, .relational_index_begin => true,
            .lake_index_lifecycle_mutate, .setting_mutate, .policy_definition_mutate, .policy_publication_mutate, .policy_publication_begin, .fk_generation_publication_begin, .fk_generation_publication_mutate, .fk_initial_create_begin, .fk_initial_create_mutate, .store_root_enroll, .store_root_enrollment_status => true,
            else => false,
        };
    }

    /// Additional body-bound read grant for principal or owner-sensitive
    /// catalog reads. The principal-independent policy publication stamp is
    /// deliberately excluded: its transport still requires an authenticated
    /// internal service, but ordinary table reads need no setting authority.
    pub fn requiresSettingAuthorityReadGrant(self: @This()) bool {
        return switch (self) {
            .lake_index_lifecycle_read,
            .lake_index_lifecycle_work,
            .setting_snapshot,
            .policy_snapshot,
            .policy_install_snapshot,
            .policy_publication_work,
            .fk_generation_publication_status,
            .fk_generation_publication_work,
            .fk_generation_publication_decision,
            .fk_generation_publication_source_decision,
            .relational_index_admission_status,
            .relational_index_owner_decision,
            .relational_index_owner_ack,
            .relational_index_owner_ack_status,
            .relational_index_publish_decide,
            .relational_index_publish_status,
            .relational_index_cancel,
            .relational_index_cancel_status,
            .relational_index_source_accept,
            .relational_index_source_complete,
            .relational_index_source_accept_status,
            .fk_initial_create_prepare,
            .fk_initial_child_decision,
            .fk_initial_create_status,
            .fk_generation_table_locked,
            .fk_initial_create_work,
            .fk_initial_parent_decision,
            => true,
            else => false,
        };
    }

    pub fn isMutation(self: @This()) bool {
        return self == .mutate or self == .fk_initial_retirement_ack or self == .relational_index_owner_ack or self == .relational_index_publish_decide or self == .relational_index_cancel or self == .relational_index_source_accept or self == .relational_index_source_complete or
            (self != .store_root_enrollment_status and self != .constraint_name_prepare and self != .constraint_name_check and self.requiresAdministrativeGrant());
    }

    pub fn isRelationalIndexOwnerCall(self: @This()) bool {
        return self == .relational_index_begin or self == .relational_index_admission_status or self == .relational_index_owner_decision or self == .relational_index_owner_ack or self == .relational_index_owner_ack_status or self == .relational_index_publish_decide or self == .relational_index_publish_status or self == .relational_index_cancel or self == .relational_index_cancel_status or self == .relational_index_source_accept or self == .relational_index_source_accept_status or self == .relational_index_source_complete;
    }
};

test "relational index publication ACK calls require body bound authority and distinguish mutations from status" {
    const abi = @import("antfly_runtime_abi");
    const VTable = struct { submit: *const fn (*const anyerror) anyerror!u64 };
    const Boundary = @import("antfly_local_sources").runtime_callback_abi.Boundary(VTable);
    const Callbacks = struct {
        fn submit(err: *const anyerror) anyerror!u64 {
            return err.*;
        }
        fn foreign(contract: *const abi.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) abi.error_abi.Status {
            return Boundary.local_dispatch(contract, callback, args, output);
        }
    };
    for ([_]anyerror{ error.IndexOwnerReceiptChanged, error.InvalidIndexOwnerReceipt, error.MetadataMutationOutcomeUnknown }) |err| {
        try std.testing.expectError(err, Boundary.call("submit", &Callbacks.foreign, &Callbacks.submit, .{&err}));
    }
    const admission: Call = .{ .relational_index_admission_status = undefined };
    const beginning: Call = .{ .relational_index_begin = undefined };
    try std.testing.expect(beginning.isMutation());
    try std.testing.expect(beginning.requiresAdministrativeGrant());
    try std.testing.expect(beginning.isRelationalIndexOwnerCall());
    try std.testing.expect(!admission.isMutation());
    try std.testing.expect(admission.requiresSettingAuthorityReadGrant());
    try std.testing.expect(admission.isRelationalIndexOwnerCall());
    const ack: Call = .{ .relational_index_owner_ack = undefined };
    const status: Call = .{ .relational_index_owner_ack_status = undefined };
    const accept: Call = .{ .relational_index_source_accept = undefined };
    const accepted: Call = .{ .relational_index_source_accept_status = undefined };
    const completed: Call = .{ .relational_index_source_complete = undefined };
    const decide: Call = .{ .relational_index_publish_decide = undefined };
    const decided: Call = .{ .relational_index_publish_status = undefined };
    const cancel: Call = .{ .relational_index_cancel = undefined };
    const canceled: Call = .{ .relational_index_cancel_status = undefined };
    try std.testing.expect(cancel.isMutation());
    try std.testing.expect(!canceled.isMutation());
    for ([_]Call{ ack, status, accept, accepted, completed, decide, decided, cancel, canceled }) |call| {
        try std.testing.expect(call.requiresSettingAuthorityReadGrant());
        try std.testing.expect(!call.requiresAdministrativeGrant());
        try std.testing.expect(call.isRelationalIndexOwnerCall());
    }
    try std.testing.expect(ack.isMutation());
    try std.testing.expect(!status.isMutation());
    try std.testing.expect(accept.isMutation());
    try std.testing.expect(completed.isMutation());
    try std.testing.expect(!accepted.isMutation());
    try std.testing.expect(decide.isMutation());
    try std.testing.expect(!decided.isMutation());
}

test "system catalog policy publication status is a service-only read, not a setting grant" {
    const status: Call = .{ .policy_publication_status = 7 };
    try std.testing.expect(!status.isMutation());
    try std.testing.expect(!status.requiresAdministrativeGrant());
    try std.testing.expect(!status.requiresSettingAuthorityReadGrant());
    try std.testing.expect((Call{ .policy_publication_work = 0 }).requiresSettingAuthorityReadGrant());
    try std.testing.expect((Call{ .policy_install_snapshot = undefined }).requiresSettingAuthorityReadGrant());
    try std.testing.expect((Call{ .setting_snapshot = undefined }).requiresSettingAuthorityReadGrant());
    try std.testing.expect((Call{ .fk_generation_publication_status = undefined }).requiresSettingAuthorityReadGrant());
    try std.testing.expect((Call{ .policy_publication_begin = undefined }).requiresAdministrativeGrant());
}

test "system catalog relational index owner decision requires a body bound read grant and cannot mutate" {
    const call: Call = .{ .relational_index_owner_decision = .{ .table_id = 1, .group_id = 2, .id = @splat(3), .operation = .fence } };
    try std.testing.expect(call.requiresSettingAuthorityReadGrant());
    try std.testing.expect(!call.requiresAdministrativeGrant());
    try std.testing.expect(!call.isMutation());
    try std.testing.expectError(error.UnknownField, std.json.parseFromSlice(Call, std.testing.allocator,
        \\{"relational_index_owner_decision":{"table_id":1,"group_id":2,"id":[3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3],"operation":"fence","index_publication_authority":true}}
    , .{}));
}

test "store-root enrollment status is an admin-bound read, not a mutation" {
    const query: Call = .{ .store_root_enrollment_status = .{
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .node_id = 3,
        .store_id = 5,
        .root_incarnation = 7,
        .public_key = @splat(1),
    } };
    try std.testing.expect(query.requiresAdministrativeGrant());
    try std.testing.expect(!query.isMutation());
}

test "native lake lifecycle calls bind private read and mutation grants" {
    const read: Call = .{ .lake_index_lifecycle_read = 4 };
    const write: Call = .{ .lake_index_lifecycle_mutate = .{ .table_id = 4, .expected_revision = 7, .mutation = .{ .release = @splat(9) } } };
    try std.testing.expect(read.requiresSettingAuthorityReadGrant());
    try std.testing.expect(!read.requiresAdministrativeGrant());
    try std.testing.expect(!read.isMutation());
    try std.testing.expect(write.requiresAdministrativeGrant());
    try std.testing.expect(write.isMutation());
    try std.testing.expect(!write.requiresSettingAuthorityReadGrant());
}

test "system catalog relation replacement requires a body-bound administrative mutation grant" {
    for ([_]Call{ .{ .constraint_name_prepare = undefined }, .{ .constraint_name_check = undefined } }) |prepare| {
        try std.testing.expect(prepare.requiresAdministrativeGrant());
        try std.testing.expect(!prepare.requiresSettingAuthorityReadGrant());
        try std.testing.expect(!prepare.isMutation());
    }
    const call: Call = .{ .relation_replace = undefined };
    try std.testing.expect(call.requiresAdministrativeGrant());
    try std.testing.expect(call.isMutation());
    try std.testing.expect(!call.requiresSettingAuthorityReadGrant());
    const schema: Call = .{ .relation_schema_mutate = undefined };
    try std.testing.expect(schema.requiresAdministrativeGrant());
    try std.testing.expect(schema.isMutation());
    try std.testing.expect(!schema.requiresSettingAuthorityReadGrant());
}

test "system catalog relation replacement wire retains exact owner and rejects identity changes" {
    const a = std.testing.allocator;
    const table: @import("../metadata/table_manager.zig").TableRecord = .{ .table_id = 7, .name = "physical" };
    var request: RelationReplacement = .{
        .guard = .{ .target = .{ .name = "idx" }, .logical_table = "logical", .owner = .{ .table_id = 7, .schema_version = 1, .schema_digest = @splat(1), .kind = .index }, .incarnation = @splat(1) },
        .expected = table,
        .replacement = table,
    };
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, Call{ .relation_replace = request }, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(Call, a, bytes, .{});
    defer parsed.deinit();
    try parsed.value.relation_replace.validate();
    try std.testing.expect(parsed.value.relation_replace.guard.owner.eql(request.guard.owner));
    try std.testing.expectEqualStrings("logical", parsed.value.relation_replace.guard.logical_table);
    // Old peers cannot silently discard the guard and execute an ordinary CAS.
    const LegacyCall = union(enum) { resolve_many: ResolveMany, mutate: Request };
    try std.testing.expectError(error.UnknownField, std.json.parseFromSlice(LegacyCall, a, bytes, .{}));
    request.replacement.table_id = 8;
    try std.testing.expectError(error.InvalidCatalogMutation, request.validate());
    request.replacement = table;
    request.replacement.name = "renamed";
    try std.testing.expectError(error.InvalidCatalogMutation, request.validate());
    request.replacement = table;
    request.guard.incarnation = @splat(0);
    try std.testing.expectError(error.InvalidCatalogMutation, request.validate());
}
