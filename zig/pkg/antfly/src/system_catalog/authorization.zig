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

//! Private, body-bound SQL authorization catalog envelopes. A role name is
//! resolved once; mutations and recovery retain immutable principal instances.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const records = local.system_catalog_privileges;
pub const outcomes = @import("sequence_outcomes.zig");
pub const Principal = records.Principal;
pub const Scope = records.Scope;
pub const Role = records.Role;
pub const Record = records.Record;
const A = std.mem.Allocator;
pub const max_targets = 256;
pub const max_changes = 4096;

pub const Actor = struct {
    session_user: Principal,
    current_role: ?Principal = null,
    credential: []const u8,

    pub fn principal(self: Actor) Principal {
        return self.current_role orelse self.session_user;
    }
    pub fn validate(self: Actor) !void {
        try self.session_user.validate();
        if (self.session_user.kind != .bootstrap and self.session_user.kind != .user) return error.InvalidCatalogMutation;
        if (self.current_role) |role| {
            try role.validate();
            if (role.kind != .role) return error.InvalidCatalogMutation;
        }
        if (self.credential.len > 512 or !std.unicode.utf8ValidateSlice(self.credential) or std.mem.indexOfScalar(u8, self.credential, 0) != null or
            (self.session_user.kind != .bootstrap and self.credential.len == 0)) return error.InvalidCatalogMutation;
    }
};
pub const Read = struct {
    pub const Namespace = struct { database: []const u8, name: []const u8 };
    namespaces: []const Namespace = &.{},
    role_names: []const []const u8 = &.{},
    scopes: []const Scope = &.{},
    principal: ?Principal = null,
    expected_revision: ?u64 = null,

    pub fn validate(self: Read) !void {
        if (self.role_names.len > max_targets or self.scopes.len > max_targets or self.namespaces.len > max_targets) return error.CatalogCommandTooLarge;
        for (self.namespaces) |namespace| try (local.system_catalog_domain.Target{ .database = namespace.database, .namespace = namespace.name, .table = "scope" }).validate();
        for (self.role_names) |name| try records.validateRoleName(name);
        for (self.scopes) |scope| try scope.validate();
        if (self.principal) |value| try value.validate();
    }
};
pub const ObjectSummary = struct {
    scope: Scope,
    generation: u64,
    owner: Principal,
    access: ?records.Access = null,
    pub fn validate(self: ObjectSummary, request: Read) !void {
        try self.scope.validate();
        try self.owner.validate();
        if (self.generation == 0 or self.owner.kind == .public) return error.InvalidSqlBackendResponse;
        if (self.scope == .defaults and !self.scope.defaults.owner.eql(self.owner)) return error.InvalidSqlBackendResponse;
        if ((self.access != null) != (request.principal != null)) return error.InvalidSqlBackendResponse;
        if (self.access) |value| {
            const mask: u16 = if (self.scope == .role) records.Access.bit(.membership) else 255;
            if ((value.rights | value.grant_options) & ~mask != 0) return error.InvalidSqlBackendResponse;
        }
    }
};
pub const Snapshot = struct {
    revision: u64,
    next_id: u64,
    head: u64,
    incarnation: [16]u8,
    roles: []const ?Role,
    objects: []const ?ObjectSummary,
    inherited: []const Role = &.{},
    namespace_ids: []const u64 = &.{},

    pub fn validate(self: Snapshot, a: A, request: Read) !void {
        try request.validate();
        if (self.next_id < 3 or std.mem.allEqual(u8, &self.incarnation, 0) or self.roles.len != request.role_names.len or self.objects.len != request.scopes.len or self.namespace_ids.len != request.namespaces.len or self.inherited.len > records.max_principals) return error.InvalidSqlBackendResponse;
        if (request.expected_revision) |revision| if (self.revision != revision) return error.CatalogGenerationChanged;
        for (self.namespace_ids) |id| if (id == 0 or id >= self.next_id) return error.InvalidSqlBackendResponse;
        for (self.roles, request.role_names) |role, name| if (role) |value| {
            try value.validate();
            if (value.id >= self.next_id or !std.mem.eql(u8, value.name, name)) return error.InvalidSqlBackendResponse;
        };
        for (self.objects, request.scopes) |object, scope| if (object) |value| {
            try value.validate(request);
            if (!value.scope.eql(scope)) return error.InvalidSqlBackendResponse;
        };
        var seen = std.AutoHashMapUnmanaged([16]u8, void).empty;
        defer seen.deinit(a);
        for (self.inherited) |role| {
            try role.validate();
            if (role.id >= self.next_id or (try seen.getOrPut(a, role.instance)).found_existing) return error.InvalidSqlBackendResponse;
        }
        if (request.principal == null and self.inherited.len != 0) return error.InvalidSqlBackendResponse;
    }
};

pub const Binding = struct { scope: Scope, generation: u64 };
pub const Mutation = struct {
    admission: outcomes.Admission,
    incarnation: [16]u8,
    actor: Actor,
    /// Derived only after native policy authorization by trusted ingress.
    /// The internal transport additionally binds its grant to the whole body.
    native_role_admin: bool = false,
    label: []const u8,
    operation: union(enum) {
        create_role: struct { name: []const u8 },
        drop_role: struct { name: []const u8, expected: ?Role, if_exists: bool = false },
        change: struct {
            targets: []const Binding,
            native_administrators: []const bool,
            grantees: []const Principal,
            privileges: []const records.Privilege,
            revoke: bool = false,
            option: bool = false,
            option_only: bool = false,
            cascade: bool = false,
        },
    },
    pub fn validate(self: Mutation) !void {
        try self.admission.validate();
        try self.actor.validate();
        if (std.mem.allEqual(u8, &self.incarnation, 0) or self.label.len == 0 or self.label.len > 1024 or !std.unicode.utf8ValidateSlice(self.label) or std.mem.indexOfScalar(u8, self.label, 0) != null) return error.InvalidCatalogMutation;
        switch (self.operation) {
            .create_role => |create| try records.validateRoleName(create.name),
            .drop_role => |drop| {
                try records.validateRoleName(drop.name);
                if (drop.expected) |role| {
                    try role.validate();
                    if (!std.mem.eql(u8, role.name, drop.name)) return error.InvalidCatalogMutation;
                } else if (!drop.if_exists) return error.InvalidCatalogMutation;
            },
            .change => |change| {
                if (change.targets.len == 0 or change.targets.len > max_targets or change.native_administrators.len != change.targets.len or change.grantees.len == 0 or change.grantees.len > max_targets or change.privileges.len == 0 or change.privileges.len > 9 or
                    change.targets.len * change.grantees.len * change.privileges.len > max_changes or
                    (change.revoke and change.option) or (!change.revoke and (change.option_only or change.cascade))) return error.InvalidCatalogMutation;
                for (change.targets, 0..) |target, position| {
                    for (change.targets[0..position]) |prior| if (target.scope.eql(prior.scope)) return error.InvalidCatalogMutation;
                    try target.scope.validate();
                    for (change.privileges) |privilege| if ((privilege == .membership) != (target.scope == .role)) return error.InvalidCatalogMutation;
                }
                for (change.grantees) |principal| {
                    try principal.validate();
                    if (principal.kind == .bootstrap) return error.InvalidCatalogMutation;
                }
                for (change.privileges, 0..) |privilege, i| for (change.privileges[0..i]) |prior| if (privilege == prior) return error.InvalidCatalogMutation;
            },
        }
    }
};
pub const Observation = struct {
    admission: outcomes.Admission,
    incarnation: [16]u8,
    actor: Actor,
    label: []const u8,
    operation: enum { create, alter, drop },
    digest: [32]u8,

    pub fn validate(self: Observation) !void {
        try self.admission.validate();
        try self.actor.validate();
        if (std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.digest, 0) or self.label.len == 0 or self.label.len > 1024 or !std.unicode.utf8ValidateSlice(self.label) or std.mem.indexOfScalar(u8, self.label, 0) != null) return error.InvalidCatalogMutation;
    }
};
pub fn observation(a: A, request: Mutation) !Observation {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:sql-authorization-command:v1\x00");
    hash.update(bytes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return .{ .admission = request.admission, .incarnation = request.incarnation, .actor = request.actor, .label = request.label, .operation = switch (request.operation) {
        .create_role => .create,
        .drop_role => .drop,
        .change => .alter,
    }, .digest = digest };
}
pub fn boundDigest(a: A, request: Observation) ![32]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:sql-authorization-observation:v1\x00");
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
pub fn roleInstance(request: Mutation) [16]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:sql-role-origin:v1\x00");
    hash.update(&request.incarnation);
    hash.update(&request.admission.nonce);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result[0..16].*;
}

pub fn command(request: Mutation) @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand {
    return .{ .version = 7, .expected_revision = 0, .mutation = .{ .kind = .role, .action = switch (request.operation) {
        .create_role => .create,
        .drop_role => .drop,
        .change => .alter,
    }, .name = request.label }, .sql_authorization = request };
}
