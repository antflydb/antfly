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

//! Object-scoped SQL grant provenance. Names are presentation; authority is
//! bound to immutable principal incarnations and immutable catalog objects.
//! The catalog commits an entire validated delta, including cascading removals.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_grants = 65536;
pub const max_principals = 4096;
pub const Privilege = enum { select, insert, update, delete, truncate, references, trigger, maintain, membership };
pub const Principal = struct {
    kind: enum { bootstrap, user, role, public },
    instance: [16]u8,
    pub const bootstrap: Principal = .{ .kind = .bootstrap, .instance = @splat(0) };
    pub const public: Principal = .{ .kind = .public, .instance = @splat(0) };
    pub fn validate(self: Principal) !void {
        if ((self.kind == .bootstrap or self.kind == .public) != std.mem.allEqual(u8, &self.instance, 0)) return error.InvalidSqlGrant;
    }
    pub fn eql(self: Principal, other: Principal) bool {
        return self.kind == other.kind and std.mem.eql(u8, &self.instance, &other.instance);
    }
};
pub const Grant = struct {
    grantor: Principal,
    grantee: Principal,
    privilege: Privilege,
    option: bool = false,
    inherit: bool = true,
    set: bool = true,
    /// Sealed by native principal administration at admission. This roots
    /// this membership grant without granting its issuer lasting ADMIN rights.
    native_admin: bool = false,
    pub fn jsonStringify(self: Grant, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(Grant).@"struct".field_names) |field| {
            if (!std.mem.eql(u8, field, "native_admin") or self.native_admin) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: Grant) !void {
        try self.grantor.validate();
        try self.grantee.validate();
        if (self.native_admin and (self.privilege != .membership or self.grantor.kind != .user)) return error.InvalidSqlGrant;
        if ((self.privilege != .membership and (!self.inherit or !self.set)) or self.grantor.kind == .public or self.grantee.kind == .bootstrap or
            (self.grantee.kind == .public and (self.option or self.privilege == .membership))) return error.InvalidSqlGrant;
    }
    fn same(self: Grant, other: Grant) bool {
        return self.grantor.eql(other.grantor) and self.grantee.eql(other.grantee) and self.privilege == other.privilege;
    }
};
const Key = struct { principal: Principal, privilege: Privilege };
const OptionSet = std.AutoHashMapUnmanaged(Key, void);

/// Prepare adjacency once. Cascading revocation is linear in the scoped
/// grant graph, including long chains; cycles cannot manufacture a root.
fn options(a: A, owner: Principal, grants: []const Grant) !OptionSet {
    var heads = std.AutoHashMapUnmanaged(Key, usize).empty;
    defer heads.deinit(a);
    const next = try a.alloc(?usize, grants.len);
    defer a.free(next);
    for (grants, 0..) |grant, i| {
        const head = try heads.getOrPut(a, .{ .principal = grant.grantor, .privilege = grant.privilege });
        next[i] = if (head.found_existing) head.value_ptr.* else null;
        head.value_ptr.* = i;
    }
    var result: OptionSet = .empty;
    errdefer result.deinit(a);
    var queue: std.ArrayList(Key) = .empty;
    defer queue.deinit(a);
    inline for (@typeInfo(Privilege).@"enum".field_values) |value| {
        const key: Key = .{ .principal = owner, .privilege = @fromBackingInt(value) };
        try result.put(a, key, {});
        try queue.append(a, key);
    }
    for (grants) |grant| if (grant.native_admin and grant.option) {
        const key: Key = .{ .principal = grant.grantee, .privilege = grant.privilege };
        if (!(try result.getOrPut(a, key)).found_existing) try queue.append(a, key);
    };
    var index: usize = 0;
    while (index < queue.items.len) : (index += 1) {
        var edge = heads.get(queue.items[index]);
        while (edge) |i| : (edge = next[i]) {
            const grant = grants[i];
            if (!grant.option) continue;
            const key: Key = .{ .principal = grant.grantee, .privilege = grant.privilege };
            if (!(try result.getOrPut(a, key)).found_existing) try queue.append(a, key);
        }
    }
    return result;
}

pub const Access = struct {
    rights: u16 = 0,
    grant_options: u16 = 0,
    pub fn bit(privilege: Privilege) u16 {
        return @as(u16, 1) << @intCast(@backingInt(privilege));
    }
    pub fn allows(self: Access, privilege: Privilege) bool {
        return self.rights & bit(privilege) != 0;
    }
    pub fn mayGrant(self: Access, privilege: Privilege) bool {
        return self.grant_options & bit(privilege) != 0;
    }
};

/// Index one principal closure for the whole metadata cut. Each ACL is reduced
/// to fixed-size rights in O(edges + reachable grant options), without an
/// edges-times-inherited-roles loop or transferring ACL graphs to ingress.
pub const AccessResolver = struct {
    principals: std.AutoHashMapUnmanaged(Principal, void) = .empty,
    pub fn init(a: A, principals: []const Principal) !AccessResolver {
        if (principals.len > max_principals + 1) return error.SqlProgramLimitExceeded;
        var result: AccessResolver = .{};
        errdefer result.deinit(a);
        for (principals) |principal| {
            try principal.validate();
            try result.principals.put(a, principal, {});
        }
        return result;
    }
    pub fn deinit(self: *AccessResolver, a: A) void {
        self.principals.deinit(a);
    }
    /// The catalog reader has already validated this object's provenance.
    pub fn captureValidated(self: *const AccessResolver, a: A, object: Object) !Access {
        var result: Access = .{};
        for (object.grants) |grant| {
            if (grant.grantee.eql(.public) or self.principals.contains(grant.grantee)) result.rights |= Access.bit(grant.privilege);
        }
        var supported = try options(a, object.owner, object.grants);
        defer supported.deinit(a);
        var iterator = supported.keyIterator();
        while (iterator.next()) |entry| {
            if (self.principals.contains(entry.principal)) result.grant_options |= Access.bit(entry.privilege);
        }
        return result;
    }
};

pub const Object = struct {
    owner: Principal,
    grants: []const Grant = &.{},
    pub fn validate(self: Object, a: A) !void {
        try self.owner.validate();
        if (self.owner.kind == .public or self.grants.len > max_grants) return error.InvalidSqlGrant;
        var seen = std.AutoHashMapUnmanaged(struct { grantor: Principal, grantee: Principal, privilege: Privilege }, void).empty;
        defer seen.deinit(a);
        for (self.grants) |grant| {
            try grant.validate();
            if ((try seen.getOrPut(a, .{ .grantor = grant.grantor, .grantee = grant.grantee, .privilege = grant.privilege })).found_existing) return error.InvalidSqlGrant;
        }
        var supported = try options(a, self.owner, self.grants);
        defer supported.deinit(a);
        for (self.grants) |grant| if (!grant.native_admin and !supported.contains(.{ .principal = grant.grantor, .privilege = grant.privilege })) return error.InvalidSqlGrant;
    }
    /// The caller supplies its already validated inheritable role closure.
    /// Grant options remain scoped to the actual grantor; persist that role,
    /// never the caller's display name or a synthetic broad write privilege.
    pub fn allows(self: Object, principals: []const Principal, privilege: Privilege) bool {
        for (self.grants) |grant| {
            if (grant.privilege != privilege) continue;
            if (grant.grantee.eql(.public)) return true;
            for (principals) |principal| if (principal.eql(grant.grantee)) return true;
        }
        return false;
    }
    pub fn grantor(self: Object, a: A, principals: []const Principal, privilege: Privilege) !Principal {
        for (principals) |principal| if (principal.eql(self.owner)) return self.owner;
        var supported = try options(a, self.owner, self.grants);
        defer supported.deinit(a);
        for (principals) |principal| if (supported.contains(.{ .principal = principal, .privilege = privilege })) return principal;
        return error.Forbidden;
    }
    pub fn add(self: Object, a: A, grant: Grant) ![]Grant {
        try self.validate(a);
        try grant.validate();
        var supported = try options(a, self.owner, self.grants);
        defer supported.deinit(a);
        if (!grant.native_admin and !supported.contains(.{ .principal = grant.grantor, .privilege = grant.privilege })) return error.Forbidden;
        for (self.grants, 0..) |prior, i| if (prior.same(grant)) {
            const result = try a.dupe(Grant, self.grants);
            result[i].option = prior.option or grant.option;
            result[i].native_admin = prior.native_admin or grant.native_admin;
            return result;
        };
        if (self.grants.len == max_grants) return error.SqlProgramLimitExceeded;
        const result = try a.alloc(Grant, self.grants.len + 1);
        @memcpy(result[0..self.grants.len], self.grants);
        result[self.grants.len] = grant;
        return result;
    }
    /// RESTRICT is atomic: the original immutable input is never changed.
    /// Revoking an option keeps the grantee's own privilege. Alternative root
    /// paths support downstream grants even if the chosen edge is removed.
    pub fn revoke(self: Object, a: A, edge: Grant, option_only: bool, cascade: bool) ![]Grant {
        try self.validate(a);
        try edge.validate();
        var candidate: std.ArrayList(Grant) = .empty;
        defer candidate.deinit(a);
        for (self.grants) |grant| {
            if (grant.same(edge)) {
                if (option_only) {
                    var retained = grant;
                    retained.option = false;
                    try candidate.append(a, retained);
                }
            } else try candidate.append(a, grant);
        }
        var supported = try options(a, self.owner, candidate.items);
        defer supported.deinit(a);
        var count: usize = 0;
        for (candidate.items) |grant| {
            if (grant.native_admin or supported.contains(.{ .principal = grant.grantor, .privilege = grant.privilege })) {
                candidate.items[count] = grant;
                count += 1;
            } else if (!cascade) return error.SqlDependentGrant;
        }
        return a.dupe(Grant, candidate.items[0..count]);
    }
};

pub const Membership = struct {
    role: Principal,
    member: Principal,
    inherit: bool = true,
    set: bool = true,
};
pub const Closure = enum { inherit, set, all };
/// Global membership adjacency is prepared once per cut. Traversal owns no
/// borrowed display names and bounds both role fanout and temporary memory.
pub fn roleClosure(a: A, memberships: []const Membership, principal: Principal, mode: Closure) ![]Principal {
    try principal.validate();
    if (memberships.len > max_grants) return error.SqlProgramLimitExceeded;
    var heads = std.AutoHashMapUnmanaged(Principal, usize).empty;
    defer heads.deinit(a);
    const next = try a.alloc(?usize, memberships.len);
    defer a.free(next);
    for (memberships, 0..) |membership, i| {
        try membership.role.validate();
        try membership.member.validate();
        if (membership.role.kind != .role or membership.member.kind == .public or membership.member.kind == .bootstrap) return error.InvalidSqlGrant;
        const head = try heads.getOrPut(a, membership.member);
        next[i] = if (head.found_existing) head.value_ptr.* else null;
        head.value_ptr.* = i;
    }
    var seen = std.AutoHashMapUnmanaged(Principal, void).empty;
    defer seen.deinit(a);
    var result: std.ArrayList(Principal) = .empty;
    errdefer result.deinit(a);
    try seen.put(a, principal, {});
    try result.append(a, principal);
    var index: usize = 0;
    while (index < result.items.len) : (index += 1) {
        var edge = heads.get(result.items[index]);
        while (edge) |i| : (edge = next[i]) {
            const membership = memberships[i];
            if (mode == .inherit and !membership.inherit or mode == .set and !membership.set) continue;
            if ((try seen.getOrPut(a, membership.role)).found_existing) continue;
            if (result.items.len == max_principals) return error.SqlProgramLimitExceeded;
            try result.append(a, membership.role);
        }
    }
    return result.toOwnedSlice(a);
}
/// Validate an adopted membership graph in O(vertices + edges), including
/// edges with INHERIT/SET disabled. Parallel grant provenance is intentional.
pub fn validateMembershipGraph(a: A, memberships: []const Membership) !void {
    const Node = struct { degree: usize = 0, head: ?usize = null };
    var nodes = std.AutoHashMapUnmanaged(Principal, Node).empty;
    defer nodes.deinit(a);
    const next = try a.alloc(?usize, memberships.len);
    defer a.free(next);
    @memset(next, null);
    for (memberships, 0..) |edge, i| {
        try edge.role.validate();
        try edge.member.validate();
        if (edge.role.kind != .role or edge.member.kind == .public or edge.member.kind == .bootstrap) return error.InvalidSqlGrant;
        if (edge.member.kind != .role) continue;
        const parent = try nodes.getOrPutValue(a, edge.role, .{});
        parent.value_ptr.degree += 1;
        const child = try nodes.getOrPutValue(a, edge.member, .{});
        next[i] = child.value_ptr.head;
        child.value_ptr.head = i;
    }
    var queue: std.ArrayList(Principal) = .empty;
    defer queue.deinit(a);
    var iterator = nodes.iterator();
    while (iterator.next()) |entry| if (entry.value_ptr.degree == 0) try queue.append(a, entry.key_ptr.*);
    var position: usize = 0;
    while (position < queue.items.len) : (position += 1) {
        var edge = nodes.get(queue.items[position]).?.head;
        while (edge) |i| : (edge = next[i]) {
            const parent = nodes.getPtr(memberships[i].role).?;
            parent.degree -= 1;
            if (parent.degree == 0) try queue.append(a, memberships[i].role);
        }
    }
    if (queue.items.len != nodes.count()) return error.SqlRoleMembershipCycle;
}

pub fn requireAcyclic(a: A, memberships: []const Membership, addition: Membership) !void {
    try addition.role.validate();
    try addition.member.validate();
    if (addition.role.kind != .role or addition.member.kind == .public or addition.member.kind == .bootstrap) return error.InvalidSqlGrant;
    const closure = try roleClosure(a, memberships, addition.role, .all);
    defer a.free(closure);
    for (closure) |principal| if (principal.eql(addition.member)) return error.SqlRoleMembershipCycle;
}

fn testPrincipal(kind: @FieldType(Principal, "kind"), n: u8) Principal {
    return .{ .kind = kind, .instance = @splat(n) };
}
test "SQL privileges preserve independent mutation rights immutable principals and PUBLIC" {
    const a = std.testing.allocator;
    const writer = testPrincipal(.user, 1);
    const reader = testPrincipal(.user, 2);
    const other_instance = testPrincipal(.user, 3);
    const object: Object = .{ .owner = .bootstrap, .grants = &.{
        .{ .grantor = .bootstrap, .grantee = writer, .privilege = .insert },
        .{ .grantor = .bootstrap, .grantee = .public, .privilege = .select },
    } };
    try object.validate(a);
    try std.testing.expect(object.allows(&.{writer}, .insert));
    try std.testing.expect(!object.allows(&.{writer}, .update));
    try std.testing.expect(!object.allows(&.{writer}, .delete));
    try std.testing.expect(!object.allows(&.{other_instance}, .insert));
    try std.testing.expect(object.allows(&.{reader}, .select));
    try std.testing.expectError(error.InvalidSqlGrant, object.add(a, .{ .grantor = .bootstrap, .grantee = .public, .privilege = .select, .option = true }));
}
test "SQL privileges RESTRICT CASCADE and alternate grant paths preserve provenance" {
    const a = std.testing.allocator;
    const alice = testPrincipal(.role, 1);
    const bob = testPrincipal(.role, 2);
    const carol = testPrincipal(.user, 3);
    const root: Grant = .{ .grantor = .bootstrap, .grantee = alice, .privilege = .select, .option = true };
    const grants = [_]Grant{
        root,
        .{ .grantor = alice, .grantee = bob, .privilege = .select, .option = true },
        .{ .grantor = bob, .grantee = carol, .privilege = .select },
    };
    const object: Object = .{ .owner = .bootstrap, .grants = &grants };
    try object.validate(a);
    try std.testing.expectError(error.SqlDependentGrant, object.revoke(a, root, true, false));
    try std.testing.expect(grants[0].option);
    const removed = try object.revoke(a, root, true, true);
    defer a.free(removed);
    try std.testing.expectEqual(@as(usize, 1), removed.len);
    try std.testing.expect(!removed[0].option);
    try std.testing.expect((Object{ .owner = .bootstrap, .grants = removed }).allows(&.{alice}, .select));
    const alternative: Grant = .{ .grantor = .bootstrap, .grantee = bob, .privilege = .select, .option = true };
    const extended = try object.add(a, alternative);
    defer a.free(extended);
    const preserved = try (Object{ .owner = .bootstrap, .grants = extended }).revoke(a, root, true, true);
    defer a.free(preserved);
    try std.testing.expectEqual(@as(usize, 3), preserved.len);
    try std.testing.expect((Object{ .owner = .bootstrap, .grants = preserved }).allows(&.{carol}, .select));
    try std.testing.expectError(error.Forbidden, object.add(a, .{ .grantor = carol, .grantee = alice, .privilege = .insert }));
}
test "SQL privileges unsupported cycles cannot manufacture grant options" {
    const alice = testPrincipal(.role, 1);
    const bob = testPrincipal(.role, 2);
    const grants = [_]Grant{
        .{ .grantor = alice, .grantee = bob, .privilege = .select, .option = true },
        .{ .grantor = bob, .grantee = alice, .privilege = .select, .option = true },
    };
    try std.testing.expectError(error.InvalidSqlGrant, (Object{ .owner = .bootstrap, .grants = &grants }).validate(std.testing.allocator));
}
test "SQL privileges role inheritance SET and cycles are distinct" {
    const a = std.testing.allocator;
    const user = testPrincipal(.user, 1);
    const one = testPrincipal(.role, 2);
    const two = testPrincipal(.role, 3);
    const memberships = [_]Membership{
        .{ .role = one, .member = user, .inherit = false },
        .{ .role = two, .member = one, .set = false },
    };
    const inherited = try roleClosure(a, &memberships, user, .inherit);
    defer a.free(inherited);
    try std.testing.expectEqual(@as(usize, 1), inherited.len);
    const settable = try roleClosure(a, &memberships, user, .set);
    defer a.free(settable);
    try std.testing.expectEqual(@as(usize, 2), settable.len);
    const all = try roleClosure(a, &memberships, user, .all);
    defer a.free(all);
    try std.testing.expectEqual(@as(usize, 3), all.len);
    try std.testing.expectError(error.SqlRoleMembershipCycle, requireAcyclic(a, &memberships, .{ .role = one, .member = two }));
    try std.testing.expectError(error.SqlRoleMembershipCycle, requireAcyclic(a, &memberships, .{ .role = one, .member = one }));
}

test "SQL privileges ownership retains grant authority without irrevocable data privileges" {
    const a = std.testing.allocator;
    const owner = testPrincipal(.user, 1);
    const edge: Grant = .{ .grantor = owner, .grantee = owner, .privilege = .select, .option = true };
    const object: Object = .{ .owner = owner, .grants = &.{edge} };
    try std.testing.expect(object.allows(&.{owner}, .select));
    const removed = try object.revoke(a, edge, false, false);
    defer a.free(removed);
    const changed: Object = .{ .owner = owner, .grants = removed };
    try std.testing.expect(!changed.allows(&.{owner}, .select));
    try std.testing.expect((try changed.grantor(a, &.{owner}, .select)).eql(owner));
}

/// SQL roles are NOLOGIN principals. Native login credentials retain their
/// own immutable user incarnation; display names do not merge the authorities.
pub const Role = struct {
    id: u64,
    instance: [16]u8,
    name: []const u8,
    creator: Principal,
    inherit: bool = true,

    pub fn principal(self: Role) Principal {
        return .{ .kind = .role, .instance = self.instance };
    }
    pub fn validate(self: Role) !void {
        if (self.id == 0) return error.InvalidSqlGrant;
        try self.principal().validate();
        try self.creator.validate();
        if (self.creator.kind == .public) return error.InvalidSqlGrant;
        try validateRoleName(self.name);
    }
};
/// Request-owned identity and role closure captured beside table definitions.
pub const role_alias_prefix = "$antfly.sql.role:";
pub fn reservedRoleAlias(name: []const u8) bool {
    return std.mem.startsWith(u8, name, role_alias_prefix);
}
pub fn aliasPrincipal(name: []const u8) !?Principal {
    if (!reservedRoleAlias(name)) return null;
    const hex = name[role_alias_prefix.len..];
    if (hex.len != 32) return error.InvalidSqlGrant;
    var instance: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&instance, hex) catch return error.InvalidSqlGrant;
    const canonical = std.fmt.bytesToHex(instance, .lower);
    if (!std.mem.eql(u8, &canonical, hex)) return error.InvalidSqlGrant;
    const principal: Principal = .{ .kind = .role, .instance = instance };
    try principal.validate();
    return principal;
}

/// SQL table ownership and existing callable/type/view ownership use the
/// same immutable principal identity for dependency retirement.
pub fn resourceOwner(a: A, resource: anytype) !?Principal {
    if (resource.sql_owner) |owner| return owner;
    switch (resource.kind) {
        .view, .routine, .named_type, .cast => {},
        else => return null,
    }
    if (resource.source_json.len == 0 or std.mem.eql(u8, resource.source_json, "null")) return null;
    var parsed = try std.json.parseFromSlice(struct { owner: ?@import("owners.zig").Owner = null }, a, resource.source_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const owner = parsed.value.owner orelse return null;
    return switch (owner) {
        .auth_disabled => null,
        .user => |user| .{ .kind = .user, .instance = user.instance_id },
        .role => |role| .{ .kind = .role, .instance = role.instance_id },
    };
}

pub fn roleAlias(a: A, role: Role) ![]u8 {
    try role.validate();
    return std.fmt.allocPrint(a, "{s}{s}", .{ role_alias_prefix, &std.fmt.bytesToHex(role.instance, .lower) });
}

pub const Authority = struct {
    principal: Principal,
    roles: []const Role,
    pub fn validate(self: Authority, a: A, expected: Principal) !void {
        try expected.validate();
        if (!self.principal.eql(expected) or self.roles.len > max_principals) return error.InvalidSqlGrant;
        var seen = std.AutoHashMapUnmanaged([16]u8, void).empty;
        defer seen.deinit(a);
        for (self.roles) |role| {
            try role.validate();
            if ((try seen.getOrPut(a, role.instance)).found_existing) return error.InvalidSqlGrant;
        }
    }
    pub fn deinit(self: Authority, a: A) void {
        for (self.roles) |role| a.free(role.name);
        a.free(self.roles);
    }
};

pub fn validateRoleName(name: []const u8) !void {
    if (name.len == 0 or name.len > 63 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSqlGrant;
}
pub fn validateNewRoleName(name: []const u8) !void {
    try validateRoleName(name);
    // These are byte-sensitive PostgreSQL role names. Quoted "PUBLIC" and
    // "Public" are ordinary roles; only the unquoted PUBLIC recipient is
    // represented by Principal.public and can authorize every principal.
    if (std.mem.eql(u8, name, "public") or std.mem.eql(u8, name, "none") or std.mem.startsWith(u8, name, "pg_")) return error.SqlReservedRoleName;
}

pub const Scope = union(enum) {
    table: u64,
    role: u64,
    defaults: struct { owner: Principal, namespace_id: u64 = 0 },

    pub fn validate(self: Scope) !void {
        switch (self) {
            .table, .role => |id| if (id == 0) return error.InvalidSqlGrant,
            .defaults => |value| {
                try value.owner.validate();
                if (value.owner.kind == .public) return error.InvalidSqlGrant;
            },
        }
    }
    pub fn eql(self: Scope, other: Scope) bool {
        return std.meta.eql(self, other);
    }
};
pub const Record = struct {
    scope: Scope,
    generation: u64,
    owner: Principal,
    grants: []const Grant = &.{},

    pub fn object(self: Record) Object {
        return .{ .owner = self.owner, .grants = self.grants };
    }
    pub fn validate(self: Record, a: A) !void {
        if (self.generation == 0) return error.InvalidSqlGrant;
        try self.scope.validate();
        try self.object().validate(a);
        if (self.scope == .defaults and !self.owner.eql(self.scope.defaults.owner)) return error.InvalidSqlGrant;
        for (self.grants) |grant| if ((grant.privilege == .membership) != (self.scope == .role)) return error.InvalidSqlGrant;
    }
};

/// PostgreSQL's table owner starts with ordinary data privileges, separately
/// from its irrevocable ability to grant them. A persisted empty global
/// default record is significant: it can revoke even the owner's defaults.
pub fn initialTablePrivileges(a: A, owner: Principal) ![]Grant {
    try owner.validate();
    if (owner.kind == .public) return error.InvalidSqlGrant;
    if (owner.kind == .bootstrap) return a.alloc(Grant, 0);
    const result = try a.alloc(Grant, 8);
    inline for (@typeInfo(Privilege).@"enum".field_values[0..8], 0..) |value, i| {
        result[i] = .{ .grantor = owner, .grantee = owner, .privilege = @fromBackingInt(value), .option = true };
    }
    return result;
}

/// Materialize current global and schema-local defaults during table creation,
/// in its catalog transaction. Later default edits never affect older tables.
/// Schema-local REVOKE cannot subtract a globally granted privilege.
pub fn tableDefaults(a: A, table_id: u64, namespace_id: u64, owner: Principal, global: ?Record, scoped: ?Record) !Record {
    if (table_id == 0 or namespace_id == 0) return error.InvalidSqlGrant;
    try owner.validate();
    var grants: std.ArrayList(Grant) = .empty;
    errdefer grants.deinit(a);
    const Edge = struct { grantor: Principal, grantee: Principal, privilege: Privilege };
    var positions = std.AutoHashMapUnmanaged(Edge, usize).empty;
    defer positions.deinit(a);
    for ([_]?Record{ global, scoped }, 0..) |layer, i| {
        if (layer) |record| {
            try record.validate(a);
            if (record.scope != .defaults or !record.owner.eql(owner) or
                record.scope.defaults.namespace_id != (if (i == 0) @as(u64, 0) else namespace_id)) return error.InvalidSqlGrant;
            for (record.grants) |grant| {
                const slot = try positions.getOrPut(a, .{ .grantor = grant.grantor, .grantee = grant.grantee, .privilege = grant.privilege });
                if (slot.found_existing) {
                    grants.items[slot.value_ptr.*].option = grants.items[slot.value_ptr.*].option or grant.option;
                } else {
                    if (grants.items.len == max_grants) return error.SqlProgramLimitExceeded;
                    slot.value_ptr.* = grants.items.len;
                    try grants.append(a, grant);
                }
            }
        } else if (i == 0) {
            const initial = try initialTablePrivileges(a, owner);
            defer a.free(initial);
            for (initial) |grant| {
                try positions.put(a, .{ .grantor = grant.grantor, .grantee = grant.grantee, .privilege = grant.privilege }, grants.items.len);
                try grants.append(a, grant);
            }
        }
    }
    const result: Record = .{ .scope = .{ .table = table_id }, .generation = 1, .owner = owner, .grants = grants.items };
    try result.validate(a);
    return .{ .scope = result.scope, .generation = 1, .owner = owner, .grants = try grants.toOwnedSlice(a) };
}

test "SQL privileges defaults materialize immutable global and schema layers including empty owner defaults" {
    const a = std.testing.allocator;
    const owner = testPrincipal(.user, 1);
    const reader = testPrincipal(.role, 2);
    const global: Record = .{ .scope = .{ .defaults = .{ .owner = owner } }, .generation = 1, .owner = owner, .grants = &.{.{ .grantor = owner, .grantee = reader, .privilege = .select }} };
    const scoped: Record = .{ .scope = .{ .defaults = .{ .owner = owner, .namespace_id = 9 } }, .generation = 2, .owner = owner, .grants = &.{.{ .grantor = owner, .grantee = reader, .privilege = .select, .option = true }} };
    const table = try tableDefaults(a, 3, 9, owner, global, scoped);
    defer a.free(table.grants);
    try std.testing.expectEqual(@as(usize, 1), table.grants.len);
    try std.testing.expect(table.grants[0].option);
    const empty: Record = .{ .scope = global.scope, .generation = 2, .owner = owner };
    const older = try tableDefaults(a, 4, 9, owner, global, emptySchemaDefaults(owner, 9));
    defer a.free(older.grants);
    try std.testing.expect(older.object().allows(&.{reader}, .select));
    const newer = try tableDefaults(a, 5, 9, owner, empty, null);
    defer a.free(newer.grants);
    try std.testing.expectEqual(@as(usize, 0), newer.grants.len);
    try std.testing.expect(!newer.object().allows(&.{owner}, .select));
    // A later default revocation cannot mutate an already prepared afterimage.
    try std.testing.expect(table.object().allows(&.{reader}, .select));
    const first = try tableDefaults(a, 6, 9, owner, null, null);
    defer a.free(first.grants);
    try std.testing.expectEqual(@as(usize, 8), first.grants.len);
    try std.testing.expect(first.object().allows(&.{owner}, .insert));
    try std.testing.expectError(error.InvalidSqlGrant, tableDefaults(a, 7, 8, owner, global, scoped));
}
fn emptySchemaDefaults(owner: Principal, namespace_id: u64) Record {
    return .{ .scope = .{ .defaults = .{ .owner = owner, .namespace_id = namespace_id } }, .generation = 1, .owner = owner };
}

test "SQL privileges role names preserve case sensitive reserved names and incarnations" {
    try validateNewRoleName("PUBLIC");
    try validateNewRoleName("Public");
    try validateNewRoleName("None");
    try std.testing.expectError(error.SqlReservedRoleName, validateNewRoleName("public"));
    try std.testing.expectError(error.SqlReservedRoleName, validateNewRoleName("none"));
    try std.testing.expectError(error.SqlReservedRoleName, validateNewRoleName("pg_system"));
    try std.testing.expectError(error.InvalidSqlGrant, validateNewRoleName("bad\x00role"));
    const role: Role = .{ .id = 3, .instance = @splat(1), .name = "readonly", .creator = .bootstrap };
    try role.validate();
    try std.testing.expectEqual(.role, role.principal().kind);
    try std.testing.expect(!role.principal().eql(testPrincipal(.user, 1)));
}

pub const Change = struct {
    grantees: []const Principal,
    privileges: []const Privilege,
    grantors: []const Principal,
    revoke: bool = false,
    option: bool = false,
    option_only: bool = false,
    cascade: bool = false,
    inherit: bool = true,
    set: bool = true,
    native_admin: bool = false,
};
const GrantKey = struct { grantor: Principal, grantee: Principal, privilege: Privilege };

/// A whole object's grant matrix is prepared once, then published atomically.
/// Bulk GRANT/REVOKE does not repeatedly clone the ACL or recompute reachability
/// for each recipient. Graph work is O(existing edges + changed edges).
pub fn apply(a: A, object: Object, change: Change) ![]Grant {
    try object.validate(a);
    if (change.privileges.len != change.grantors.len or change.privileges.len == 0 or change.grantees.len == 0 or change.privileges.len > 9 or change.grantees.len > 256 or change.privileges.len * change.grantees.len > 4096 or
        (change.revoke and change.option) or (!change.revoke and (change.option_only or change.cascade))) return error.InvalidSqlGrant;
    var supported = try options(a, object.owner, object.grants);
    defer supported.deinit(a);
    var selected = std.AutoHashMapUnmanaged(GrantKey, void).empty;
    defer selected.deinit(a);
    for (change.privileges, change.grantors) |privilege, grantor| {
        if (!change.native_admin and !supported.contains(.{ .principal = grantor, .privilege = privilege })) return error.Forbidden;
        for (change.grantees) |grantee| {
            try (Grant{ .grantor = grantor, .grantee = grantee, .privilege = privilege, .option = change.option, .inherit = change.inherit, .set = change.set, .native_admin = change.native_admin }).validate();
            try selected.put(a, .{ .grantor = grantor, .grantee = grantee, .privilege = privilege }, {});
        }
    }
    var result: std.ArrayList(Grant) = .empty;
    errdefer result.deinit(a);
    try result.ensureTotalCapacity(a, @min(max_grants, object.grants.len + if (change.revoke) @as(usize, 0) else selected.count()));
    for (object.grants) |grant| {
        const key: GrantKey = .{ .grantor = grant.grantor, .grantee = grant.grantee, .privilege = grant.privilege };
        if (!selected.remove(key)) {
            try result.append(a, grant);
            continue;
        }
        if (change.revoke and !change.option_only) continue;
        var updated = grant;
        updated.option = if (change.revoke) false else grant.option or change.option;
        if (!change.revoke) updated.native_admin = grant.native_admin or change.native_admin;
        try result.append(a, updated);
    }
    if (!change.revoke) {
        // Preserve input order independently of hash-table bucket order.
        for (change.privileges, change.grantors) |privilege, grantor| for (change.grantees) |grantee| {
            if (!selected.remove(.{ .grantor = grantor, .grantee = grantee, .privilege = privilege })) continue;
            if (result.items.len == max_grants) return error.SqlProgramLimitExceeded;
            try result.append(a, .{ .grantor = grantor, .grantee = grantee, .privilege = privilege, .option = change.option, .inherit = change.inherit, .set = change.set, .native_admin = change.native_admin });
        };
    } else {
        var after = try options(a, object.owner, result.items);
        defer after.deinit(a);
        var retained: usize = 0;
        for (result.items) |grant| {
            if (grant.native_admin or after.contains(.{ .principal = grant.grantor, .privilege = grant.privilege })) {
                result.items[retained] = grant;
                retained += 1;
            } else if (!change.cascade) return error.SqlDependentGrant;
        }
        result.shrinkRetainingCapacity(retained);
    }
    return result.toOwnedSlice(a);
}

pub fn grantors(a: A, object: Object, principals: []const Principal, privileges: []const Privilege) ![]Principal {
    var supported = try options(a, object.owner, object.grants);
    defer supported.deinit(a);
    const result = try a.alloc(Principal, privileges.len);
    errdefer a.free(result);
    for (privileges, result) |privilege, *out| {
        out.* = for (principals) |principal| {
            if (supported.contains(.{ .principal = principal, .privilege = privilege })) break principal;
        } else return error.Forbidden;
    }
    return result;
}

test "SQL privileges bulk matrix changes retain per privilege provenance and atomic cascade" {
    const a = std.testing.allocator;
    const alice = testPrincipal(.user, 1);
    const bob = testPrincipal(.role, 2);
    const carol = testPrincipal(.user, 3);
    const seeded = try apply(a, .{ .owner = .bootstrap }, .{ .grantees = &.{ alice, bob }, .privileges = &.{ .select, .insert }, .grantors = &.{ .bootstrap, .bootstrap }, .option = true });
    defer a.free(seeded);
    try std.testing.expectEqual(@as(usize, 4), seeded.len);
    const delegated = try apply(a, .{ .owner = .bootstrap, .grants = seeded }, .{ .grantees = &.{carol}, .privileges = &.{ .select, .insert }, .grantors = &.{ alice, bob } });
    defer a.free(delegated);
    const object: Object = .{ .owner = .bootstrap, .grants = delegated };
    try std.testing.expectError(error.SqlDependentGrant, apply(a, object, .{ .grantees = &.{ alice, bob }, .privileges = &.{ .select, .insert }, .grantors = &.{ .bootstrap, .bootstrap }, .revoke = true, .option_only = true }));
    const removed = try apply(a, object, .{ .grantees = &.{ alice, bob }, .privileges = &.{ .select, .insert }, .grantors = &.{ .bootstrap, .bootstrap }, .revoke = true, .option_only = true, .cascade = true });
    defer a.free(removed);
    try std.testing.expectEqual(@as(usize, 4), removed.len);
    const changed: Object = .{ .owner = .bootstrap, .grants = removed };
    try std.testing.expect(!changed.allows(&.{carol}, .select));
    try std.testing.expect(!changed.allows(&.{carol}, .insert));
    try std.testing.expect(changed.allows(&.{alice}, .select));
    try std.testing.expect(changed.allows(&.{bob}, .insert));
    try object.validate(a);
}

pub fn retireMembershipPrincipal(a: A, object: Object, principal: Principal) ![]Grant {
    try object.validate(a);
    if (object.owner.eql(principal)) return error.SqlDependentRole;
    var result: std.ArrayList(Grant) = .empty;
    errdefer result.deinit(a);
    for (object.grants) |grant| {
        if (grant.privilege != .membership) return error.InvalidSqlGrant;
        if (!grant.grantor.eql(principal) and !grant.grantee.eql(principal)) try result.append(a, grant);
    }
    var supported = try options(a, object.owner, result.items);
    defer supported.deinit(a);
    var retained: usize = 0;
    for (result.items) |grant| if (grant.native_admin or supported.contains(.{ .principal = grant.grantor, .privilege = grant.privilege })) {
        result.items[retained] = grant;
        retained += 1;
    };
    result.shrinkRetainingCapacity(retained);
    return result.toOwnedSlice(a);
}

test "SQL privileges restored membership graphs reject cycles regardless of inheritance options" {
    const a = std.testing.allocator;
    const one = testPrincipal(.role, 1);
    const two = testPrincipal(.role, 2);
    const three = testPrincipal(.role, 3);
    const dag = [_]Membership{
        .{ .role = one, .member = two },
        .{ .role = one, .member = two, .inherit = false, .set = false },
        .{ .role = two, .member = three },
        .{ .role = one, .member = testPrincipal(.user, 9) },
    };
    try validateMembershipGraph(a, &dag);
    const cycle = dag ++ [_]Membership{.{ .role = three, .member = one, .inherit = false, .set = false }};
    try std.testing.expectError(error.SqlRoleMembershipCycle, validateMembershipGraph(a, &cycle));
    try std.testing.expectError(error.SqlRoleMembershipCycle, validateMembershipGraph(a, &.{.{ .role = one, .member = one }}));
    try std.testing.checkAllAllocationFailures(a, struct {
        fn run(failing: A, edges: []const Membership) !void {
            try validateMembershipGraph(failing, edges);
        }
    }.run, .{@as([]const Membership, &dag)});
}

test "SQL privileges compact access preserves PUBLIC inherited data rights and direct grant authority" {
    const a = std.testing.allocator;
    const owner = testPrincipal(.user, 1);
    const user = testPrincipal(.user, 2);
    const role = testPrincipal(.role, 3);
    const object: Object = .{ .owner = owner, .grants = &.{
        .{ .grantor = owner, .grantee = .public, .privilege = .select },
        .{ .grantor = owner, .grantee = role, .privilege = .insert, .option = true },
    } };
    try object.validate(a);
    var resolver = try AccessResolver.init(a, &.{ user, role });
    defer resolver.deinit(a);
    const access = try resolver.captureValidated(a, object);
    try std.testing.expect(access.allows(.select));
    try std.testing.expect(access.allows(.insert));
    try std.testing.expect(!access.allows(.update));
    try std.testing.expect(!access.allows(.delete));
    try std.testing.expect(access.mayGrant(.insert));
    try std.testing.expect(!access.mayGrant(.select));
    var direct = try AccessResolver.init(a, &.{user});
    defer direct.deinit(a);
    try std.testing.expect(!(try direct.captureValidated(a, object)).mayGrant(.insert));
    var owning = try AccessResolver.init(a, &.{owner});
    defer owning.deinit(a);
    const owning_access = try owning.captureValidated(a, object);
    try std.testing.expect(!owning_access.allows(.insert));
    try std.testing.expect(owning_access.mayGrant(.insert));
}

test "SQL privileges native role administrators retain independent grantors without minting ADMIN" {
    const a = std.testing.allocator;
    const alice = testPrincipal(.user, 1);
    const bob = testPrincipal(.user, 2);
    const member = testPrincipal(.user, 3);
    const leaf = testPrincipal(.user, 4);
    const first = try apply(a, .{ .owner = .bootstrap }, .{ .grantees = &.{member}, .privileges = &.{.membership}, .grantors = &.{alice}, .option = true, .native_admin = true });
    defer a.free(first);
    const both = try apply(a, .{ .owner = .bootstrap, .grants = first }, .{ .grantees = &.{member}, .privileges = &.{.membership}, .grantors = &.{bob}, .option = true, .native_admin = true });
    defer a.free(both);
    try std.testing.expectEqual(@as(usize, 2), both.len);
    const object: Object = .{ .owner = .bootstrap, .grants = both };
    try std.testing.expectError(error.Forbidden, object.grantor(a, &.{alice}, .membership));
    const delegated = try apply(a, object, .{ .grantees = &.{leaf}, .privileges = &.{.membership}, .grantors = &.{member} });
    defer a.free(delegated);
    const remaining = try apply(a, .{ .owner = .bootstrap, .grants = delegated }, .{ .grantees = &.{member}, .privileges = &.{.membership}, .grantors = &.{alice}, .native_admin = true, .revoke = true });
    defer a.free(remaining);
    const supported: Object = .{ .owner = .bootstrap, .grants = remaining };
    try supported.validate(a);
    try std.testing.expect(supported.allows(&.{leaf}, .membership));
    try std.testing.expectError(error.SqlDependentGrant, apply(a, supported, .{ .grantees = &.{member}, .privileges = &.{.membership}, .grantors = &.{bob}, .native_admin = true, .revoke = true }));
    const cascaded = try apply(a, supported, .{ .grantees = &.{member}, .privileges = &.{.membership}, .grantors = &.{bob}, .native_admin = true, .revoke = true, .cascade = true });
    defer a.free(cascaded);
    try std.testing.expectEqual(@as(usize, 0), cascaded.len);
}
