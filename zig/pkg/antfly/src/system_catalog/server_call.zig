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

//! Server catalog command envelope and admission policy.
const std = @import("std");
const domain = @import("domain.zig");
const Read = domain.Read;
const Request = domain.Request;
const ResolveMany = domain.ResolveMany;
const TableList = domain.TableList;
const TableStatusTarget = domain.TableStatusTarget;
const Target = domain.Target;

pub const Call = union(enum) {
    setting_snapshot: @import("settings.zig").Scope,
    policy_snapshot: @import("policies.zig").SnapshotRequest,
    policy_install_snapshot: @import("policies.zig").InstallRequest,
    policy_publication_status: u64,
    /// Narrow, linearizable supervisor work queue; excludes policy definitions.
    policy_publication_work: u64,
    /// Trusted metadata ingress derives the owner cut; callers supply no
    /// owner descriptors or policy bundle bytes.
    policy_publication_begin: @import("policies.zig").BeginRequest,
    /// Administrator-authored draft definition. Never activates enforcement.
    policy_definition_mutate: @import("policies.zig").Command,
    /// Private coordinator-only transition. Never accepted from public SQL.
    policy_publication_mutate: @import("policies.zig").PublicationCommand,
    fk_generation_publication_begin: @import("../metadata/fk_generation_publication.zig").Plan,
    fk_generation_publication_mutate: @import("../metadata/fk_generation_publication.zig").Command,
    fk_generation_publication_status: u64,
    fk_generation_publication_work: u64,
    fk_generation_publication_decision: @import("../metadata/fk_generation_publication.zig").DecisionRequest,
    fk_generation_publication_source_decision: @import("../metadata/fk_generation_publication.zig").SourceDecisionRequest,
    fk_initial_create_prepare: @import("../metadata/fk_generation_publication.zig").InitialCreatePrepareRequest,
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
    setting_mutate: @import("settings.zig").Request,
    list_tables: TableList,
    export_snapshot: void,
    read: Read,
    snapshot: void,
    resolve: Target,
    resolve_many: ResolveMany,
    query_definition: []const u8,
    mutate: Request,
    // A distinct operation makes older peers reject unsupported point reads.
    table_status: TableStatusTarget,
    write_validation: []const u8,
    write_validation_revision: void,

    pub fn requiresAdministrativeGrant(self: @This()) bool {
        return switch (self) {
            .setting_mutate, .policy_definition_mutate, .policy_publication_mutate, .policy_publication_begin, .fk_generation_publication_begin, .fk_generation_publication_mutate, .fk_initial_create_begin, .fk_initial_create_mutate, .store_root_enroll, .store_root_enrollment_status => true,
            else => false,
        };
    }

    /// Additional body-bound read grant for principal or owner-sensitive
    /// catalog reads. The principal-independent policy publication stamp is
    /// deliberately excluded: its transport still requires an authenticated
    /// internal service, but ordinary table reads need no setting authority.
    pub fn requiresSettingAuthorityReadGrant(self: @This()) bool {
        return switch (self) {
            .setting_snapshot,
            .policy_snapshot,
            .policy_install_snapshot,
            .policy_publication_work,
            .fk_generation_publication_status,
            .fk_generation_publication_work,
            .fk_generation_publication_decision,
            .fk_generation_publication_source_decision,
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
        return self == .mutate or self == .fk_initial_retirement_ack or
            (self != .store_root_enrollment_status and self.requiresAdministrativeGrant());
    }
};

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
