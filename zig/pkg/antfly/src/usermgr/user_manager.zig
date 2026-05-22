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
const casbin = @import("antfly_casbin");

const Allocator = std.mem.Allocator;
const bcrypt = std.crypto.pwhash.bcrypt;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const default_rbac_model_text =
    \\[request_definition]
    \\r = sub, typ, obj, act
    \\[policy_definition]
    \\p = sub, typ, obj, act
    \\p2 = sub, obj, filter
    \\[role_definition]
    \\g = _, _
    \\[matchers]
    \\m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
;

pub const ResourceType = enum {
    table,
    user,
    @"*",

    pub fn fromSlice(raw: []const u8) !ResourceType {
        if (std.mem.eql(u8, raw, "table")) return .table;
        if (std.mem.eql(u8, raw, "user")) return .user;
        if (std.mem.eql(u8, raw, "*")) return .@"*";
        return error.InvalidResourceType;
    }

    pub fn slice(self: ResourceType) []const u8 {
        return switch (self) {
            .table => "table",
            .user => "user",
            .@"*" => "*",
        };
    }
};

pub const PermissionType = enum {
    read,
    write,
    admin,

    pub fn fromSlice(raw: []const u8) !PermissionType {
        if (std.mem.eql(u8, raw, "read")) return .read;
        if (std.mem.eql(u8, raw, "write")) return .write;
        if (std.mem.eql(u8, raw, "admin")) return .admin;
        return error.InvalidPermissionType;
    }

    pub fn slice(self: PermissionType) []const u8 {
        return @tagName(self);
    }
};

pub const Permission = struct {
    resource: []u8,
    resource_type: ResourceType,
    type: PermissionType,

    pub fn initOwned(
        alloc: Allocator,
        resource_type: ResourceType,
        resource: []const u8,
        permission_type: PermissionType,
    ) !Permission {
        return .{
            .resource = try alloc.dupe(u8, resource),
            .resource_type = resource_type,
            .type = permission_type,
        };
    }

    pub fn deinit(self: *Permission, alloc: Allocator) void {
        alloc.free(self.resource);
        self.* = undefined;
    }
};

pub const User = struct {
    username: []u8,
    password_hash: []u8,
    metadata_json: []u8 = &.{},

    pub fn clone(self: User, alloc: Allocator) !User {
        return .{
            .username = try alloc.dupe(u8, self.username),
            .password_hash = try alloc.dupe(u8, self.password_hash),
            .metadata_json = if (self.metadata_json.len > 0) try alloc.dupe(u8, self.metadata_json) else &.{},
        };
    }

    pub fn deinit(self: *User, alloc: Allocator) void {
        alloc.free(self.username);
        alloc.free(self.password_hash);
        if (self.metadata_json.len > 0) alloc.free(self.metadata_json);
        self.* = undefined;
    }
};

pub const RowFilterEntry = struct {
    table: []u8,
    filter: []u8,

    pub fn initOwned(alloc: Allocator, table: []const u8, filter: []const u8) !RowFilterEntry {
        return .{
            .table = try alloc.dupe(u8, table),
            .filter = try alloc.dupe(u8, filter),
        };
    }

    pub fn deinit(self: *RowFilterEntry, alloc: Allocator) void {
        alloc.free(self.table);
        alloc.free(self.filter);
        self.* = undefined;
    }
};

pub const AuthSubjectKind = enum {
    user,
    role,
    group,
    subject,

    pub fn slice(self: AuthSubjectKind) []const u8 {
        return @tagName(self);
    }
};

pub const AuthSubjectEntry = struct {
    subject: []u8,
    kind: AuthSubjectKind,

    pub fn initOwned(alloc: Allocator, subject: []const u8, kind: AuthSubjectKind) !AuthSubjectEntry {
        return .{
            .subject = try alloc.dupe(u8, subject),
            .kind = kind,
        };
    }

    pub fn deinit(self: *AuthSubjectEntry, alloc: Allocator) void {
        alloc.free(self.subject);
        self.* = undefined;
    }
};

pub const ApiKey = struct {
    key_id: []u8,
    username: []u8,
    name: []u8,
    permissions: []Permission,
    row_filter: []RowFilterEntry,
    created_at_ns: u64,
    expires_at_ns: ?u64 = null,

    pub fn clone(self: ApiKey, alloc: Allocator) !ApiKey {
        var permissions = try alloc.alloc(Permission, self.permissions.len);
        errdefer alloc.free(permissions);
        var filled_perms: usize = 0;
        errdefer {
            for (permissions[0..filled_perms]) |*perm| perm.deinit(alloc);
        }
        for (self.permissions, 0..) |perm, i| {
            permissions[i] = try Permission.initOwned(alloc, perm.resource_type, perm.resource, perm.type);
            filled_perms += 1;
        }

        var row_filter = try alloc.alloc(RowFilterEntry, self.row_filter.len);
        errdefer alloc.free(row_filter);
        var filled_filters: usize = 0;
        errdefer {
            for (row_filter[0..filled_filters]) |*entry| entry.deinit(alloc);
        }
        for (self.row_filter, 0..) |entry, i| {
            row_filter[i] = try RowFilterEntry.initOwned(alloc, entry.table, entry.filter);
            filled_filters += 1;
        }

        return .{
            .key_id = try alloc.dupe(u8, self.key_id),
            .username = try alloc.dupe(u8, self.username),
            .name = try alloc.dupe(u8, self.name),
            .permissions = permissions,
            .row_filter = row_filter,
            .created_at_ns = self.created_at_ns,
            .expires_at_ns = self.expires_at_ns,
        };
    }

    pub fn deinit(self: *ApiKey, alloc: Allocator) void {
        alloc.free(self.key_id);
        alloc.free(self.username);
        alloc.free(self.name);
        for (self.permissions) |*perm| perm.deinit(alloc);
        alloc.free(self.permissions);
        for (self.row_filter) |*entry| entry.deinit(alloc);
        alloc.free(self.row_filter);
        self.* = undefined;
    }
};

pub const CreatedApiKey = struct {
    key: ApiKey,
    key_secret: []u8,
    encoded: []u8,

    pub fn deinit(self: *CreatedApiKey, alloc: Allocator) void {
        self.key.deinit(alloc);
        alloc.free(self.key_secret);
        alloc.free(self.encoded);
        self.* = undefined;
    }
};

pub const ApiKeyRecord = struct {
    key: ApiKey,
    secret_hash: []u8,
    secret_salt: []u8,

    pub fn clone(self: ApiKeyRecord, alloc: Allocator) !ApiKeyRecord {
        return .{
            .key = try self.key.clone(alloc),
            .secret_hash = try alloc.dupe(u8, self.secret_hash),
            .secret_salt = try alloc.dupe(u8, self.secret_salt),
        };
    }

    pub fn deinit(self: *ApiKeyRecord, alloc: Allocator) void {
        self.key.deinit(alloc);
        alloc.free(self.secret_hash);
        alloc.free(self.secret_salt);
        self.* = undefined;
    }

    pub fn publicClone(self: ApiKeyRecord, alloc: Allocator) !ApiKey {
        return try self.key.clone(alloc);
    }
};

pub const ValidatedApiKey = struct {
    username: []u8,
    permissions: []Permission,
    row_filter: []RowFilterEntry,
    metadata_json: []u8 = &.{},
    roles: [][]u8 = &.{},

    pub fn deinit(self: *ValidatedApiKey, alloc: Allocator) void {
        alloc.free(self.username);
        for (self.permissions) |*perm| perm.deinit(alloc);
        alloc.free(self.permissions);
        for (self.row_filter) |*entry| entry.deinit(alloc);
        alloc.free(self.row_filter);
        if (self.metadata_json.len > 0) alloc.free(self.metadata_json);
        freeOwnedStrings(alloc, self.roles);
        self.* = undefined;
    }
};

pub const UserStore = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        load_users: *const fn (ptr: *anyopaque, alloc: Allocator) anyerror![]User,
        save_user: *const fn (ptr: *anyopaque, alloc: Allocator, user: *const User) anyerror!void,
        delete_user: *const fn (ptr: *anyopaque, username: []const u8) anyerror!bool,
        load_api_keys: *const fn (ptr: *anyopaque, alloc: Allocator) anyerror![]ApiKeyRecord,
        save_api_key: *const fn (ptr: *anyopaque, alloc: Allocator, record: *const ApiKeyRecord) anyerror!void,
        delete_api_key: *const fn (ptr: *anyopaque, key_id: []const u8) anyerror!bool,
    };

    pub fn loadUsers(self: UserStore, alloc: Allocator) ![]User {
        return try self.vtable.load_users(self.ptr, alloc);
    }

    pub fn saveUser(self: UserStore, alloc: Allocator, user: *const User) !void {
        return try self.vtable.save_user(self.ptr, alloc, user);
    }

    pub fn deleteUser(self: UserStore, username: []const u8) !bool {
        return try self.vtable.delete_user(self.ptr, username);
    }

    pub fn loadApiKeys(self: UserStore, alloc: Allocator) ![]ApiKeyRecord {
        return try self.vtable.load_api_keys(self.ptr, alloc);
    }

    pub fn saveApiKey(self: UserStore, alloc: Allocator, record: *const ApiKeyRecord) !void {
        return try self.vtable.save_api_key(self.ptr, alloc, record);
    }

    pub fn deleteApiKey(self: UserStore, key_id: []const u8) !bool {
        return try self.vtable.delete_api_key(self.ptr, key_id);
    }
};

pub const MemoryStore = struct {
    alloc: Allocator,
    users: std.ArrayList(User) = .empty,
    api_keys: std.ArrayList(ApiKeyRecord) = .empty,

    const iface_vtable: UserStore.VTable = .{
        .load_users = loadUsers,
        .save_user = saveUser,
        .delete_user = deleteUser,
        .load_api_keys = loadApiKeys,
        .save_api_key = saveApiKey,
        .delete_api_key = deleteApiKey,
    };

    pub fn init(alloc: Allocator) MemoryStore {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryStore) void {
        for (self.users.items) |*user| user.deinit(self.alloc);
        self.users.deinit(self.alloc);
        for (self.api_keys.items) |*record| record.deinit(self.alloc);
        self.api_keys.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn iface(self: *MemoryStore) UserStore {
        return .{
            .ptr = self,
            .vtable = &iface_vtable,
        };
    }

    fn loadUsers(ptr: *anyopaque, alloc: Allocator) ![]User {
        const self: *MemoryStore = @ptrCast(@alignCast(ptr));
        const out = try alloc.alloc(User, self.users.items.len);
        errdefer alloc.free(out);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*user| user.deinit(alloc);
        }
        for (self.users.items, 0..) |user, i| {
            out[i] = try user.clone(alloc);
            filled += 1;
        }
        return out;
    }

    fn saveUser(ptr: *anyopaque, alloc: Allocator, user: *const User) !void {
        const self: *MemoryStore = @ptrCast(@alignCast(ptr));
        _ = alloc;
        for (self.users.items) |*existing| {
            if (!std.mem.eql(u8, existing.username, user.username)) continue;
            self.alloc.free(existing.password_hash);
            existing.password_hash = try self.alloc.dupe(u8, user.password_hash);
            if (existing.metadata_json.len > 0) self.alloc.free(existing.metadata_json);
            existing.metadata_json = if (user.metadata_json.len > 0) try self.alloc.dupe(u8, user.metadata_json) else &.{};
            return;
        }
        try self.users.append(self.alloc, try user.clone(self.alloc));
    }

    fn deleteUser(ptr: *anyopaque, username: []const u8) !bool {
        const self: *MemoryStore = @ptrCast(@alignCast(ptr));
        var i: usize = 0;
        while (i < self.users.items.len) {
            if (!std.mem.eql(u8, self.users.items[i].username, username)) {
                i += 1;
                continue;
            }
            self.users.items[i].deinit(self.alloc);
            _ = self.users.swapRemove(i);
            return true;
        }
        return false;
    }

    fn loadApiKeys(ptr: *anyopaque, alloc: Allocator) ![]ApiKeyRecord {
        const self: *MemoryStore = @ptrCast(@alignCast(ptr));
        const out = try alloc.alloc(ApiKeyRecord, self.api_keys.items.len);
        errdefer alloc.free(out);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*record| record.deinit(alloc);
        }
        for (self.api_keys.items, 0..) |record, i| {
            out[i] = try record.clone(alloc);
            filled += 1;
        }
        return out;
    }

    fn saveApiKey(ptr: *anyopaque, alloc: Allocator, record: *const ApiKeyRecord) !void {
        const self: *MemoryStore = @ptrCast(@alignCast(ptr));
        _ = alloc;
        for (self.api_keys.items) |*existing| {
            if (!std.mem.eql(u8, existing.key.key_id, record.key.key_id)) continue;
            existing.deinit(self.alloc);
            existing.* = try record.clone(self.alloc);
            return;
        }
        try self.api_keys.append(self.alloc, try record.clone(self.alloc));
    }

    fn deleteApiKey(ptr: *anyopaque, key_id: []const u8) !bool {
        const self: *MemoryStore = @ptrCast(@alignCast(ptr));
        var i: usize = 0;
        while (i < self.api_keys.items.len) {
            if (!std.mem.eql(u8, self.api_keys.items[i].key.key_id, key_id)) {
                i += 1;
                continue;
            }
            self.api_keys.items[i].deinit(self.alloc);
            _ = self.api_keys.swapRemove(i);
            return true;
        }
        return false;
    }
};

pub const UserManager = struct {
    alloc: Allocator,
    store: UserStore,
    enforcer: casbin.Enforcer,
    users: std.StringHashMapUnmanaged([]u8) = .{},
    user_metadata: std.StringHashMapUnmanaged([]u8) = .{},
    api_keys: std.StringHashMapUnmanaged(ApiKeyRecord) = .{},

    pub fn init(alloc: Allocator, store: UserStore, enforcer: casbin.Enforcer) !UserManager {
        var manager = UserManager{
            .alloc = alloc,
            .store = store,
            .enforcer = enforcer,
        };
        errdefer manager.deinit();

        const loaded = try store.loadUsers(alloc);
        defer {
            for (loaded) |*user| user.deinit(alloc);
            alloc.free(loaded);
        }

        for (loaded) |user| {
            try manager.users.put(alloc, try alloc.dupe(u8, user.username), try alloc.dupe(u8, user.password_hash));
            try manager.user_metadata.put(
                alloc,
                try alloc.dupe(u8, user.username),
                if (user.metadata_json.len > 0) try alloc.dupe(u8, user.metadata_json) else try alloc.dupe(u8, "{}"),
            );
        }

        const loaded_api_keys = try store.loadApiKeys(alloc);
        defer {
            for (loaded_api_keys) |*record| record.deinit(alloc);
            alloc.free(loaded_api_keys);
        }
        for (loaded_api_keys) |record| {
            try manager.api_keys.put(alloc, try alloc.dupe(u8, record.key.key_id), try record.clone(alloc));
        }
        manager.enforcer.enableAutoSave(true);
        return manager;
    }

    pub fn deinit(self: *UserManager) void {
        self.enforcer.deinit();
        var it = self.users.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.users.deinit(self.alloc);
        var metadata_it = self.user_metadata.iterator();
        while (metadata_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.user_metadata.deinit(self.alloc);
        var api_key_it = self.api_keys.iterator();
        while (api_key_it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.alloc);
        }
        self.api_keys.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn createUser(
        self: *UserManager,
        username: []const u8,
        password: []const u8,
        initial_policies: []const Permission,
    ) !User {
        return try self.createUserWithMetadata(username, password, initial_policies, "{}");
    }

    pub fn createUserWithMetadata(
        self: *UserManager,
        username: []const u8,
        password: []const u8,
        initial_policies: []const Permission,
        metadata_json: []const u8,
    ) !User {
        if (self.users.contains(username)) return error.UserExists;

        var stored = blk: {
            const password_hash = try hashPassword(self.alloc, password);
            errdefer self.alloc.free(password_hash);
            const normalized_metadata = try normalizeMetadataJson(self.alloc, metadata_json);
            errdefer self.alloc.free(normalized_metadata);
            const owned_username = try self.alloc.dupe(u8, username);
            errdefer self.alloc.free(owned_username);
            break :blk User{
                .username = owned_username,
                .password_hash = password_hash,
                .metadata_json = normalized_metadata,
            };
        };
        errdefer stored.deinit(self.alloc);

        try self.store.saveUser(self.alloc, &stored);
        errdefer {
            if (self.users.fetchRemove(username)) |removed| {
                self.alloc.free(removed.key);
                self.alloc.free(removed.value);
            }
            if (self.user_metadata.fetchRemove(username)) |removed| {
                self.alloc.free(removed.key);
                self.alloc.free(removed.value);
            }
            _ = self.store.deleteUser(username) catch {};
        }
        try self.users.put(self.alloc, try self.alloc.dupe(u8, stored.username), try self.alloc.dupe(u8, stored.password_hash));
        try self.user_metadata.put(self.alloc, try self.alloc.dupe(u8, stored.username), try self.alloc.dupe(u8, stored.metadata_json));

        if (initial_policies.len > 0) {
            var policy_fields = try self.alloc.alloc([]const []const u8, initial_policies.len);
            defer self.alloc.free(policy_fields);
            var policy_storage = try self.alloc.alloc([4][]const u8, initial_policies.len);
            defer self.alloc.free(policy_storage);
            for (initial_policies, 0..) |perm, i| {
                policy_storage[i] = .{
                    username,
                    perm.resource_type.slice(),
                    perm.resource,
                    perm.type.slice(),
                };
                policy_fields[i] = policy_storage[i][0..];
            }
            _ = try self.enforcer.addPolicies(policy_fields);
        }

        const result = try stored.clone(self.alloc);
        stored.deinit(self.alloc);
        return result;
    }

    pub fn getUser(self: *const UserManager, username: []const u8) !User {
        const password_hash = self.users.get(username) orelse return error.UserNotFound;
        const metadata_json = self.user_metadata.get(username) orelse "{}";
        return .{
            .username = try self.alloc.dupe(u8, username),
            .password_hash = try self.alloc.dupe(u8, password_hash),
            .metadata_json = try self.alloc.dupe(u8, metadata_json),
        };
    }

    pub fn authenticateUser(self: *const UserManager, username: []const u8, password: []const u8) !User {
        const password_hash = self.users.get(username) orelse return error.UserNotFound;
        const metadata_json = self.user_metadata.get(username) orelse "{}";
        try verifyPassword(password_hash, password);
        return .{
            .username = try self.alloc.dupe(u8, username),
            .password_hash = try self.alloc.dupe(u8, password_hash),
            .metadata_json = try self.alloc.dupe(u8, metadata_json),
        };
    }

    pub fn updatePassword(self: *UserManager, username: []const u8, new_password: []const u8) !void {
        const existing = self.users.getPtr(username) orelse return error.UserNotFound;
        const metadata_json = self.user_metadata.get(username) orelse "{}";
        const new_hash = try hashPassword(self.alloc, new_password);
        errdefer self.alloc.free(new_hash);
        var stored = User{
            .username = @constCast(username),
            .password_hash = new_hash,
            .metadata_json = @constCast(metadata_json),
        };
        try self.store.saveUser(self.alloc, &stored);
        self.alloc.free(existing.*);
        existing.* = new_hash;
    }

    pub fn deleteUser(self: *UserManager, username: []const u8) !void {
        const removed = self.users.fetchRemove(username) orelse return error.UserNotFound;
        defer {
            self.alloc.free(removed.key);
            self.alloc.free(removed.value);
        }
        if (self.user_metadata.fetchRemove(username)) |metadata| {
            self.alloc.free(metadata.key);
            self.alloc.free(metadata.value);
        }
        _ = try self.store.deleteUser(username);
        var owned_key_ids = std.ArrayList([]u8).empty;
        defer {
            for (owned_key_ids.items) |key_id| self.alloc.free(key_id);
            owned_key_ids.deinit(self.alloc);
        }
        var api_key_it = self.api_keys.iterator();
        while (api_key_it.next()) |entry| {
            if (!std.mem.eql(u8, entry.value_ptr.key.username, username)) continue;
            try owned_key_ids.append(self.alloc, try self.alloc.dupe(u8, entry.key_ptr.*));
        }
        for (owned_key_ids.items) |key_id| try self.deleteApiKey(username, key_id);
        _ = try self.enforcer.removeFilteredPolicy(0, &.{username});
        _ = try self.enforcer.removeFilteredGroupingPolicy(0, &.{username});
        _ = try self.enforcer.removeFilteredNamedPolicy("p2", 0, &.{username});
    }

    pub fn listUsers(self: *const UserManager) ![][]u8 {
        var out = try self.alloc.alloc([]u8, self.users.count());
        errdefer self.alloc.free(out);
        var i: usize = 0;
        var it = self.users.keyIterator();
        while (it.next()) |username| : (i += 1) {
            out[i] = try self.alloc.dupe(u8, username.*);
        }
        return out;
    }

    pub fn enforce(
        self: *const UserManager,
        username: []const u8,
        resource_type: ResourceType,
        resource: []const u8,
        permission_type: PermissionType,
    ) !bool {
        if (!self.users.contains(username)) return error.UserNotFound;
        return try self.enforcer.enforce(username, resource_type.slice(), resource, permission_type.slice());
    }

    pub fn addPermissionToSubject(self: *UserManager, subject: []const u8, permission: Permission) !void {
        _ = try self.enforcer.addPolicy(&.{
            subject,
            permission.resource_type.slice(),
            permission.resource,
            permission.type.slice(),
        });
    }

    pub fn addPermissionToUser(self: *UserManager, username: []const u8, permission: Permission) !void {
        if (!self.users.contains(username)) return error.UserNotFound;
        try self.addPermissionToSubject(username, permission);
    }

    pub fn removePermissionFromUser(
        self: *UserManager,
        username: []const u8,
        resource_name: []const u8,
        resource_type: ResourceType,
    ) !void {
        if (!self.users.contains(username)) return error.UserNotFound;
        const removed = if (std.mem.eql(u8, resource_name, "*"))
            try self.enforcer.removeFilteredPolicy(0, &.{ username, resource_type.slice() })
        else
            try self.enforcer.removeFilteredPolicy(0, &.{ username, resource_type.slice(), resource_name });
        if (!removed) return error.RoleNotFound;
    }

    pub fn getPermissionsForUser(self: *const UserManager, username: []const u8) ![]Permission {
        if (!self.users.contains(username)) return error.UserNotFound;
        const rules = try self.enforcer.getPermissionsForUser(self.alloc, username);
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }

        var out = std.ArrayList(Permission).empty;
        errdefer {
            for (out.items) |*perm| perm.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        for (rules) |rule| {
            if (rule.fields.len < 4) continue;
            try out.append(self.alloc, try Permission.initOwned(
                self.alloc,
                try ResourceType.fromSlice(rule.fields[1]),
                rule.fields[2],
                try PermissionType.fromSlice(rule.fields[3]),
            ));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn addRoleToSubject(self: *UserManager, subject: []const u8, role: []const u8) !void {
        if (subject.len == 0 or role.len == 0) return error.InvalidRole;
        _ = try self.enforcer.addNamedPolicy("g", &.{ subject, role });
    }

    pub fn addRoleToUser(self: *UserManager, username: []const u8, role: []const u8) !void {
        if (!self.users.contains(username)) return error.UserNotFound;
        try self.addRoleToSubject(username, role);
    }

    pub fn removeRoleFromSubject(self: *UserManager, subject: []const u8, role: []const u8) !void {
        const removed = try self.enforcer.removeFilteredNamedPolicy("g", 0, &.{ subject, role });
        if (!removed) return error.RoleNotFound;
    }

    pub fn removeRoleFromUser(self: *UserManager, username: []const u8, role: []const u8) !void {
        if (!self.users.contains(username)) return error.UserNotFound;
        try self.removeRoleFromSubject(username, role);
    }

    pub fn getRolesForUser(self: *const UserManager, username: []const u8) ![][]u8 {
        if (!self.users.contains(username)) return error.UserNotFound;

        var queue = std.ArrayList([]const u8).empty;
        defer queue.deinit(self.alloc);
        var visited = std.StringHashMapUnmanaged(void){};
        defer {
            var it = visited.iterator();
            while (it.next()) |entry| self.alloc.free(entry.key_ptr.*);
            visited.deinit(self.alloc);
        }
        var out = std.ArrayList([]u8).empty;
        errdefer {
            for (out.items) |role| self.alloc.free(role);
            out.deinit(self.alloc);
        }

        const owned_username = try self.alloc.dupe(u8, username);
        visited.put(self.alloc, owned_username, {}) catch |err| {
            self.alloc.free(owned_username);
            return err;
        };
        try queue.append(self.alloc, owned_username);

        var index: usize = 0;
        while (index < queue.items.len) : (index += 1) {
            const current = queue.items[index];
            const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "g", 0, &.{current});
            defer {
                for (rules) |*rule| rule.deinit(self.alloc);
                self.alloc.free(rules);
            }

            for (rules) |rule| {
                if (rule.fields.len < 2) continue;
                const role = rule.fields[1];
                if (visited.contains(role)) continue;
                const owned_role = try self.alloc.dupe(u8, role);
                visited.put(self.alloc, owned_role, {}) catch |err| {
                    self.alloc.free(owned_role);
                    return err;
                };
                try queue.append(self.alloc, owned_role);
                const out_role = try self.alloc.dupe(u8, role);
                out.append(self.alloc, out_role) catch |err| {
                    self.alloc.free(out_role);
                    return err;
                };
            }
        }

        return try out.toOwnedSlice(self.alloc);
    }

    pub fn listAuthSubjects(self: *const UserManager) ![]AuthSubjectEntry {
        var subjects = std.StringArrayHashMapUnmanaged(AuthSubjectKind){};
        defer {
            var it = subjects.iterator();
            while (it.next()) |entry| self.alloc.free(entry.key_ptr.*);
            subjects.deinit(self.alloc);
        }

        var user_it = self.users.keyIterator();
        while (user_it.next()) |username| {
            try putAuthSubject(self.alloc, &subjects, username.*, .user);
        }

        try self.collectAuthSubjectsFromPolicy(&subjects, "p");
        try self.collectAuthSubjectsFromPolicy(&subjects, "p2");
        try self.collectAuthSubjectsFromPolicy(&subjects, "g");

        var out = std.ArrayList(AuthSubjectEntry).empty;
        errdefer {
            for (out.items) |*entry| entry.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        var it = subjects.iterator();
        while (it.next()) |entry| {
            try out.append(self.alloc, try AuthSubjectEntry.initOwned(self.alloc, entry.key_ptr.*, entry.value_ptr.*));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn collectAuthSubjectsFromPolicy(
        self: *const UserManager,
        subjects: *std.StringArrayHashMapUnmanaged(AuthSubjectKind),
        ptype: []const u8,
    ) !void {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, ptype, 0, &.{});
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        for (rules) |rule| {
            if (rule.fields.len == 0) continue;
            try putAuthSubject(self.alloc, subjects, rule.fields[0], inferAuthSubjectKind(rule.fields[0]));
            if (std.mem.eql(u8, ptype, "g") and rule.fields.len >= 2) {
                try putAuthSubject(self.alloc, subjects, rule.fields[1], inferAuthSubjectKind(rule.fields[1]));
            }
        }
    }

    pub fn setSubjectRowFilter(self: *UserManager, subject: []const u8, table: []const u8, filter_json: []const u8) !void {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, filter_json, .{});
        parsed.deinit();
        _ = try self.enforcer.removeFilteredNamedPolicy("p2", 0, &.{ subject, table });
        _ = try self.enforcer.addNamedPolicy("p2", &.{ subject, table, filter_json });
    }

    pub fn setRowFilter(self: *UserManager, username: []const u8, table: []const u8, filter_json: []const u8) !void {
        if (!self.users.contains(username)) return error.UserNotFound;
        try self.setSubjectRowFilter(username, table, filter_json);
    }

    pub fn removeSubjectRowFilter(self: *UserManager, subject: []const u8, table: []const u8) !void {
        const removed = try self.enforcer.removeFilteredNamedPolicy("p2", 0, &.{ subject, table });
        if (!removed) return error.RowFilterNotFound;
    }

    pub fn removeRowFilter(self: *UserManager, username: []const u8, table: []const u8) !void {
        if (!self.users.contains(username)) return error.UserNotFound;
        try self.removeSubjectRowFilter(username, table);
    }

    pub fn getSubjectRowFilter(self: *const UserManager, subject: []const u8, table: []const u8) ![]u8 {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p2", 0, &.{ subject, table });
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        if (rules.len == 0 or rules[0].fields.len < 3) return error.RowFilterNotFound;
        return try self.alloc.dupe(u8, rules[0].fields[2]);
    }

    pub fn getRowFilter(self: *const UserManager, username: []const u8, table: []const u8) ![]u8 {
        if (!self.users.contains(username)) return error.UserNotFound;
        return try self.getSubjectRowFilter(username, table);
    }

    pub fn listSubjectRowFilters(self: *const UserManager, subject: []const u8) ![]RowFilterEntry {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p2", 0, &.{subject});
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        var out = std.ArrayList(RowFilterEntry).empty;
        errdefer {
            for (out.items) |*entry| entry.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        for (rules) |rule| {
            if (rule.fields.len < 3) continue;
            try out.append(self.alloc, try RowFilterEntry.initOwned(self.alloc, rule.fields[1], rule.fields[2]));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn listRowFilters(self: *const UserManager, username: []const u8) ![]RowFilterEntry {
        if (!self.users.contains(username)) return error.UserNotFound;
        return try self.listSubjectRowFilters(username);
    }

    pub fn getRowFilters(self: *const UserManager, username: []const u8) ![]RowFilterEntry {
        if (!self.users.contains(username)) return error.UserNotFound;
        const listed = try self.listSubjectRowFilters(username);
        defer {
            for (listed) |*entry| entry.deinit(self.alloc);
            self.alloc.free(listed);
        }
        const roles = try self.getRolesForUser(username);
        defer freeOwnedStrings(self.alloc, roles);

        var merged = std.StringArrayHashMapUnmanaged([]u8){};
        defer {
            var it = merged.iterator();
            while (it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            merged.deinit(self.alloc);
        }

        for (listed) |entry| {
            try mergeRowFilterEntry(self.alloc, &merged, entry);
        }

        for (roles) |role| {
            const role_filters = try self.listSubjectRowFilters(role);
            defer {
                for (role_filters) |*entry| entry.deinit(self.alloc);
                self.alloc.free(role_filters);
            }
            for (role_filters) |entry| {
                try mergeRowFilterEntry(self.alloc, &merged, entry);
            }
        }

        var out = std.ArrayList(RowFilterEntry).empty;
        errdefer {
            for (out.items) |*entry| entry.deinit(self.alloc);
            out.deinit(self.alloc);
        }

        var it = merged.iterator();
        while (it.next()) |entry| {
            try out.append(self.alloc, .{
                .table = try self.alloc.dupe(u8, entry.key_ptr.*),
                .filter = try self.alloc.dupe(u8, entry.value_ptr.*),
            });
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn createApiKey(
        self: *UserManager,
        username: []const u8,
        name: []const u8,
        permissions: []const Permission,
        row_filter: []const RowFilterEntry,
        expires_at_ns: ?u64,
    ) !CreatedApiKey {
        if (!self.users.contains(username)) return error.UserNotFound;

        for (permissions) |perm| {
            const allowed = try self.enforce(username, perm.resource_type, perm.resource, perm.type);
            if (!allowed) return error.PrivilegeEscalation;
        }

        for (row_filter) |entry| {
            var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, entry.filter, .{});
            parsed.deinit();
        }

        const key_id = try generateRandomAlphanumeric(self.alloc, 20);
        defer self.alloc.free(key_id);
        const secret_raw = try randomBytes(self.alloc, 16);
        defer self.alloc.free(secret_raw);
        const key_secret = try encodeBase64UrlNoPad(self.alloc, secret_raw);
        errdefer self.alloc.free(key_secret);
        const salt = try randomBytes(self.alloc, 16);
        const secret_hash = try hashApiKeySecret(self.alloc, salt, secret_raw);

        var api_key = ApiKey{
            .key_id = try self.alloc.dupe(u8, key_id),
            .username = try self.alloc.dupe(u8, username),
            .name = try self.alloc.dupe(u8, name),
            .permissions = try clonePermissions(self.alloc, permissions),
            .row_filter = try cloneRowFilters(self.alloc, row_filter),
            .created_at_ns = nowNs(),
            .expires_at_ns = expires_at_ns,
        };
        errdefer api_key.deinit(self.alloc);

        var record = ApiKeyRecord{
            .key = api_key,
            .secret_hash = secret_hash,
            .secret_salt = salt,
        };
        defer record.deinit(self.alloc);

        try self.store.saveApiKey(self.alloc, &record);
        try self.api_keys.put(self.alloc, try self.alloc.dupe(u8, key_id), try record.clone(self.alloc));

        const encoded = try encodeBasicCredential(self.alloc, key_id, key_secret);
        return .{
            .key = try record.publicClone(self.alloc),
            .key_secret = key_secret,
            .encoded = encoded,
        };
    }

    pub fn validateApiKey(self: *const UserManager, key_id: []const u8, key_secret: []const u8) !ValidatedApiKey {
        const record = self.api_keys.get(key_id) orelse return error.ApiKeyNotFound;
        const secret_size = std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(key_secret) catch {
            return error.ApiKeyInvalid;
        };
        const secret_raw = try self.alloc.alloc(u8, secret_size);
        defer self.alloc.free(secret_raw);
        std.base64.url_safe_no_pad.Decoder.decode(secret_raw, key_secret) catch {
            return error.ApiKeyInvalid;
        };
        const computed_hash = try hashApiKeySecret(self.alloc, record.secret_salt, secret_raw);
        defer self.alloc.free(computed_hash);
        if (!std.mem.eql(u8, computed_hash, record.secret_hash)) return error.ApiKeyInvalid;
        if (record.key.expires_at_ns) |expires_at_ns| {
            if (nowNs() > expires_at_ns) return error.ApiKeyExpired;
        }
        const owner_row_filter = try self.getRowFilters(record.key.username);
        defer {
            for (owner_row_filter) |*entry| entry.deinit(self.alloc);
            self.alloc.free(owner_row_filter);
        }

        return .{
            .username = try self.alloc.dupe(u8, record.key.username),
            .permissions = if (record.key.permissions.len > 0)
                try clonePermissions(self.alloc, record.key.permissions)
            else
                try self.getPermissionsForUser(record.key.username),
            .row_filter = try combineLayeredRowFilters(self.alloc, owner_row_filter, record.key.row_filter),
            .metadata_json = try self.alloc.dupe(u8, self.user_metadata.get(record.key.username) orelse "{}"),
            .roles = try self.getRolesForUser(record.key.username),
        };
    }

    pub fn listApiKeys(self: *const UserManager, username: []const u8) ![]ApiKey {
        if (!self.users.contains(username)) return error.UserNotFound;
        var out = std.ArrayList(ApiKey).empty;
        errdefer {
            for (out.items) |*item| item.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        var it = self.api_keys.iterator();
        while (it.next()) |entry| {
            if (!std.mem.eql(u8, entry.value_ptr.key.username, username)) continue;
            try out.append(self.alloc, try entry.value_ptr.publicClone(self.alloc));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn deleteApiKey(self: *UserManager, username: []const u8, key_id: []const u8) !void {
        const record = self.api_keys.get(key_id) orelse return error.ApiKeyNotFound;
        if (!std.mem.eql(u8, record.key.username, username)) return error.ApiKeyNotFound;
        const removed = self.api_keys.fetchRemove(key_id) orelse return error.ApiKeyNotFound;
        defer {
            self.alloc.free(removed.key);
            var owned = removed.value;
            owned.deinit(self.alloc);
        }
        _ = try self.store.deleteApiKey(key_id);
    }
};

pub fn ensureDefaultAdminUser(manager: *UserManager) !void {
    var existing = manager.getUser("admin") catch |err| switch (err) {
        error.UserNotFound => {
            var admin_permission = [_]Permission{
                try Permission.initOwned(manager.alloc, .@"*", "*", .admin),
            };
            defer admin_permission[0].deinit(manager.alloc);
            var user = try manager.createUser("admin", "admin", &admin_permission);
            user.deinit(manager.alloc);
            return;
        },
        else => return err,
    };
    existing.deinit(manager.alloc);
}

pub fn initDefaultEnforcer(alloc: Allocator, adapter: casbin.Adapter) !casbin.Enforcer {
    return try casbin.Enforcer.init(alloc, try casbin.Model.fromString(alloc, default_rbac_model_text), adapter);
}

fn normalizeMetadataJson(alloc: Allocator, metadata_json: []const u8) ![]u8 {
    const raw = if (metadata_json.len == 0) "{}" else metadata_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidMetadata;
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}

fn hashPassword(alloc: Allocator, password: []const u8) ![]u8 {
    var salt: [bcrypt.salt_length]u8 = undefined;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    try io_impl.io().randomSecure(&salt);
    var buf: [256]u8 = undefined;
    const hashed = try bcrypt.strHashWithSalt(
        password,
        .{ .params = bcrypt.Params.owasp, .encoding = .phc },
        &buf,
        salt,
    );
    return try alloc.dupe(u8, hashed);
}

fn verifyPassword(password_hash: []const u8, password: []const u8) !void {
    bcrypt.strVerify(password_hash, password, .{ .silently_truncate_password = false }) catch {
        return error.InvalidPassword;
    };
}

fn clonePermissions(alloc: Allocator, permissions: []const Permission) ![]Permission {
    const out = try alloc.alloc(Permission, permissions.len);
    errdefer alloc.free(out);
    var filled: usize = 0;
    errdefer for (out[0..filled]) |*perm| perm.deinit(alloc);
    for (permissions, 0..) |perm, i| {
        out[i] = try Permission.initOwned(alloc, perm.resource_type, perm.resource, perm.type);
        filled += 1;
    }
    return out;
}

fn cloneRowFilters(alloc: Allocator, row_filter: []const RowFilterEntry) ![]RowFilterEntry {
    const out = try alloc.alloc(RowFilterEntry, row_filter.len);
    errdefer alloc.free(out);
    var filled: usize = 0;
    errdefer for (out[0..filled]) |*entry| entry.deinit(alloc);
    for (row_filter, 0..) |entry, i| {
        out[i] = try RowFilterEntry.initOwned(alloc, entry.table, entry.filter);
        filled += 1;
    }
    return out;
}

fn freeOwnedStrings(alloc: Allocator, values: []const []u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(@constCast(values));
}

fn mergeRowFilterEntry(
    alloc: Allocator,
    merged: *std.StringArrayHashMapUnmanaged([]u8),
    entry: RowFilterEntry,
) !void {
    const gop = try merged.getOrPut(alloc, entry.table);
    if (!gop.found_existing) {
        gop.key_ptr.* = try alloc.dupe(u8, entry.table);
        gop.value_ptr.* = try alloc.dupe(u8, entry.filter);
        return;
    }

    const combined = try std.fmt.allocPrint(
        alloc,
        "{{\"conjuncts\":[{s},{s}]}}",
        .{ gop.value_ptr.*, entry.filter },
    );
    alloc.free(gop.value_ptr.*);
    gop.value_ptr.* = combined;
}

fn inferAuthSubjectKind(subject: []const u8) AuthSubjectKind {
    if (std.mem.startsWith(u8, subject, "role:")) return .role;
    if (std.mem.startsWith(u8, subject, "group:")) return .group;
    return .subject;
}

fn authSubjectKindRank(kind: AuthSubjectKind) u8 {
    return switch (kind) {
        .subject => 0,
        .role, .group => 1,
        .user => 2,
    };
}

fn putAuthSubject(
    alloc: Allocator,
    subjects: *std.StringArrayHashMapUnmanaged(AuthSubjectKind),
    subject: []const u8,
    kind: AuthSubjectKind,
) !void {
    if (subject.len == 0) return;
    const owned_subject = try alloc.dupe(u8, subject);
    errdefer alloc.free(owned_subject);
    const gop = try subjects.getOrPut(alloc, owned_subject);
    if (gop.found_existing) {
        alloc.free(owned_subject);
        if (authSubjectKindRank(kind) > authSubjectKindRank(gop.value_ptr.*)) {
            gop.value_ptr.* = kind;
        }
        return;
    }
    gop.key_ptr.* = owned_subject;
    gop.value_ptr.* = kind;
}

fn combineLayeredRowFilters(
    alloc: Allocator,
    base: []const RowFilterEntry,
    overlay: []const RowFilterEntry,
) ![]RowFilterEntry {
    var tables = std.StringArrayHashMapUnmanaged(void){};
    defer {
        var it = tables.iterator();
        while (it.next()) |entry| alloc.free(entry.key_ptr.*);
        tables.deinit(alloc);
    }

    try collectExplicitRowFilterTables(alloc, &tables, base);
    try collectExplicitRowFilterTables(alloc, &tables, overlay);

    var out = std.ArrayList(RowFilterEntry).empty;
    errdefer {
        for (out.items) |*entry| entry.deinit(alloc);
        out.deinit(alloc);
    }

    var table_it = tables.iterator();
    while (table_it.next()) |entry| {
        if (try combineSelectedRowFilterForTable(alloc, base, overlay, entry.key_ptr.*)) |filter| {
            const table = alloc.dupe(u8, entry.key_ptr.*) catch |err| {
                alloc.free(filter);
                return err;
            };
            var out_entry = RowFilterEntry{
                .table = table,
                .filter = filter,
            };
            out.append(alloc, out_entry) catch |err| {
                out_entry.deinit(alloc);
                return err;
            };
        }
    }

    if (try combineExactRowFilterForTable(alloc, base, overlay, "*")) |filter| {
        const table = alloc.dupe(u8, "*") catch |err| {
            alloc.free(filter);
            return err;
        };
        var out_entry = RowFilterEntry{
            .table = table,
            .filter = filter,
        };
        out.append(alloc, out_entry) catch |err| {
            out_entry.deinit(alloc);
            return err;
        };
    }

    return try out.toOwnedSlice(alloc);
}

fn collectExplicitRowFilterTables(
    alloc: Allocator,
    tables: *std.StringArrayHashMapUnmanaged(void),
    row_filters: []const RowFilterEntry,
) !void {
    for (row_filters) |entry| {
        if (std.mem.eql(u8, entry.table, "*")) continue;
        const gop = try tables.getOrPut(alloc, entry.table);
        if (!gop.found_existing) {
            gop.key_ptr.* = try alloc.dupe(u8, entry.table);
            gop.value_ptr.* = {};
        }
    }
}

fn combineSelectedRowFilterForTable(
    alloc: Allocator,
    base: []const RowFilterEntry,
    overlay: []const RowFilterEntry,
    table: []const u8,
) !?[]u8 {
    return try combineOptionalRowFilterJson(
        alloc,
        selectedRowFilterForTable(base, table),
        selectedRowFilterForTable(overlay, table),
    );
}

fn combineExactRowFilterForTable(
    alloc: Allocator,
    base: []const RowFilterEntry,
    overlay: []const RowFilterEntry,
    table: []const u8,
) !?[]u8 {
    return try combineOptionalRowFilterJson(
        alloc,
        exactRowFilterForTable(base, table),
        exactRowFilterForTable(overlay, table),
    );
}

fn selectedRowFilterForTable(row_filters: []const RowFilterEntry, table: []const u8) ?[]const u8 {
    if (exactRowFilterForTable(row_filters, table)) |filter| return filter;
    return exactRowFilterForTable(row_filters, "*");
}

fn exactRowFilterForTable(row_filters: []const RowFilterEntry, table: []const u8) ?[]const u8 {
    for (row_filters) |entry| {
        if (!std.mem.eql(u8, entry.table, table)) continue;
        if (std.mem.eql(u8, entry.filter, "null")) return null;
        return entry.filter;
    }
    return null;
}

fn combineOptionalRowFilterJson(
    alloc: Allocator,
    base: ?[]const u8,
    overlay: ?[]const u8,
) !?[]u8 {
    if (base) |base_filter| {
        if (overlay) |overlay_filter| {
            return try std.fmt.allocPrint(
                alloc,
                "{{\"conjuncts\":[{s},{s}]}}",
                .{ base_filter, overlay_filter },
            );
        }
        return try alloc.dupe(u8, base_filter);
    }
    if (overlay) |overlay_filter| return try alloc.dupe(u8, overlay_filter);
    return null;
}

fn randomBytes(alloc: Allocator, len: usize) ![]u8 {
    const out = try alloc.alloc(u8, len);
    errdefer alloc.free(out);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try io_impl.io().randomSecure(out);
    return out;
}

fn generateRandomAlphanumeric(alloc: Allocator, len: usize) ![]u8 {
    const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    const random = try randomBytes(alloc, len);
    defer alloc.free(random);
    const out = try alloc.alloc(u8, len);
    for (random, 0..) |byte, i| {
        out[i] = alphabet[byte % alphabet.len];
    }
    return out;
}

fn hashApiKeySecret(alloc: Allocator, salt: []const u8, secret_raw: []const u8) ![]u8 {
    var hasher = Sha256.init(.{});
    hasher.update(salt);
    hasher.update(secret_raw);
    var out: [Sha256.digest_length]u8 = undefined;
    hasher.final(&out);
    return try alloc.dupe(u8, &out);
}

fn encodeBase64UrlNoPad(alloc: Allocator, raw: []const u8) ![]u8 {
    const size = std.base64.url_safe_no_pad.Encoder.calcSize(raw.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.url_safe_no_pad.Encoder.encode(out, raw);
    return out;
}

fn encodeBasicCredential(alloc: Allocator, key_id: []const u8, key_secret: []const u8) ![]u8 {
    const joined = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ key_id, key_secret });
    defer alloc.free(joined);
    const size = std.base64.standard.Encoder.calcSize(joined.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, joined);
    return out;
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {},
        else => return 0,
    }
    const sec: u64 = @intCast(@max(ts.sec, 0));
    const nsec: u64 = @intCast(@max(ts.nsec, 0));
    return sec * std.time.ns_per_s + nsec;
}

test "usermgr create authenticate and persist users through store" {
    const alloc = std.testing.allocator;

    var store = MemoryStore.init(alloc);
    defer store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();

    var manager = try UserManager.init(
        alloc,
        store.iface(),
        try initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer manager.deinit();

    var created = try manager.createUserWithMetadata("alice", "secret", &.{}, "{\"tenant_id\":\"acme\"}");
    defer created.deinit(alloc);
    try std.testing.expectEqualStrings("alice", created.username);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"acme\"}", created.metadata_json);
    try std.testing.expect(!std.mem.eql(u8, created.password_hash, "secret"));

    var authed = try manager.authenticateUser("alice", "secret");
    defer authed.deinit(alloc);
    try std.testing.expectEqualStrings("alice", authed.username);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"acme\"}", authed.metadata_json);
    try std.testing.expectError(error.InvalidPassword, manager.authenticateUser("alice", "wrong"));

    var reloaded = try UserManager.init(
        alloc,
        store.iface(),
        try initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer reloaded.deinit();
    var loaded = try reloaded.getUser("alice");
    defer loaded.deinit(alloc);
    try std.testing.expectEqualStrings("alice", loaded.username);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"acme\"}", loaded.metadata_json);
}

test "usermgr default admin seed is idempotent and grants admin" {
    const alloc = std.testing.allocator;

    var store = MemoryStore.init(alloc);
    defer store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();

    var manager = try UserManager.init(
        alloc,
        store.iface(),
        try initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer manager.deinit();

    try ensureDefaultAdminUser(&manager);
    try ensureDefaultAdminUser(&manager);

    var authed = try manager.authenticateUser("admin", "admin");
    defer authed.deinit(alloc);
    try std.testing.expect(try manager.enforce("admin", .@"*", "*", .admin));
}

test "usermgr permissions and row filters mirror go semantics" {
    const alloc = std.testing.allocator;

    var store = MemoryStore.init(alloc);
    defer store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();

    var manager = try UserManager.init(
        alloc,
        store.iface(),
        try initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer manager.deinit();

    var initial = [_]Permission{
        try Permission.initOwned(alloc, .table, "docs", .read),
    };
    defer initial[0].deinit(alloc);

    var user = try manager.createUser("bob", "secret", &initial);
    defer user.deinit(alloc);

    try std.testing.expect(try manager.enforce("bob", .table, "docs", .read));
    try std.testing.expect(!(try manager.enforce("bob", .table, "docs", .write)));

    var extra = try Permission.initOwned(alloc, .table, "docs", .write);
    defer extra.deinit(alloc);
    try manager.addPermissionToUser("bob", extra);
    try std.testing.expect(try manager.enforce("bob", .table, "docs", .write));

    const perms = try manager.getPermissionsForUser("bob");
    defer {
        for (perms) |*perm| perm.deinit(alloc);
        alloc.free(perms);
    }
    try std.testing.expectEqual(@as(usize, 2), perms.len);

    try manager.setRowFilter("bob", "docs", "{\"term\":{\"department\":\"eng\"}}");
    try manager.setRowFilter("bob", "docs", "{\"term\":{\"region\":\"us\"}}");
    const filters = try manager.getRowFilters("bob");
    defer {
        for (filters) |*entry| entry.deinit(alloc);
        alloc.free(filters);
    }
    try std.testing.expectEqual(@as(usize, 1), filters.len);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"region\":\"us\"") != null);

    try manager.removePermissionFromUser("bob", "docs", .table);
    try std.testing.expect(!(try manager.enforce("bob", .table, "docs", .read)));
}

test "usermgr roles inherit permissions and row filters" {
    const alloc = std.testing.allocator;

    var store = MemoryStore.init(alloc);
    defer store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();

    var manager = try UserManager.init(
        alloc,
        store.iface(),
        try initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer manager.deinit();

    var user = try manager.createUserWithMetadata("alice", "secret", &.{}, "{\"tenant_id\":\"acme\"}");
    defer user.deinit(alloc);

    var read_docs = try Permission.initOwned(alloc, .table, "docs", .read);
    defer read_docs.deinit(alloc);
    try manager.addPermissionToSubject("role:tenant_reader", read_docs);
    try manager.addRoleToUser("alice", "role:tenant_reader");
    try manager.addRoleToSubject("role:tenant_reader", "group:eng");

    try std.testing.expect(try manager.enforce("alice", .table, "docs", .read));

    const roles = try manager.getRolesForUser("alice");
    defer freeOwnedStrings(alloc, roles);
    try std.testing.expectEqual(@as(usize, 2), roles.len);
    try std.testing.expect(std.mem.indexOf(u8, roles[0], "role:tenant_reader") != null or std.mem.indexOf(u8, roles[1], "role:tenant_reader") != null);
    try std.testing.expect(std.mem.indexOf(u8, roles[0], "group:eng") != null or std.mem.indexOf(u8, roles[1], "group:eng") != null);

    const subjects = try manager.listAuthSubjects();
    defer {
        for (subjects) |*entry| entry.deinit(alloc);
        alloc.free(subjects);
    }
    var found_alice = false;
    var found_role = false;
    var found_group = false;
    for (subjects) |entry| {
        if (std.mem.eql(u8, entry.subject, "alice") and entry.kind == .user) found_alice = true;
        if (std.mem.eql(u8, entry.subject, "role:tenant_reader") and entry.kind == .role) found_role = true;
        if (std.mem.eql(u8, entry.subject, "group:eng") and entry.kind == .group) found_group = true;
    }
    try std.testing.expect(found_alice);
    try std.testing.expect(found_role);
    try std.testing.expect(found_group);

    try manager.setSubjectRowFilter("role:tenant_reader", "docs", "{\"term\":{\"tenant_id\":{\"$auth\":\"metadata.tenant_id\"}}}");
    try manager.setSubjectRowFilter("group:eng", "docs", "{\"term\":{\"acl.groups\":\"eng\"}}");
    try manager.setRowFilter("alice", "docs", "{\"term\":{\"owner\":{\"$auth\":\"username\"}}}");

    const filters = try manager.getRowFilters("alice");
    defer {
        for (filters) |*entry| entry.deinit(alloc);
        alloc.free(filters);
    }
    try std.testing.expectEqual(@as(usize, 1), filters.len);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"owner\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"tenant_id\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filters[0].filter, "\"acl.groups\"") != null);

    const direct = try manager.listRowFilters("alice");
    defer {
        for (direct) |*entry| entry.deinit(alloc);
        alloc.free(direct);
    }
    try std.testing.expectEqual(@as(usize, 1), direct.len);
    try std.testing.expect(std.mem.indexOf(u8, direct[0].filter, "\"owner\"") != null);
}

test "usermgr api keys validate and persist creator-scoped permissions" {
    const alloc = std.testing.allocator;

    var store = MemoryStore.init(alloc);
    defer store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();

    var manager = try UserManager.init(
        alloc,
        store.iface(),
        try initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer manager.deinit();

    var initial = [_]Permission{
        try Permission.initOwned(alloc, .table, "docs", .read),
    };
    defer initial[0].deinit(alloc);
    var user = try manager.createUser("alice", "secret", &initial);
    defer user.deinit(alloc);
    try manager.setRowFilter("alice", "docs", "{\"term\":{\"tenant_id\":\"acme\"}}");

    var row_filter = [_]RowFilterEntry{
        try RowFilterEntry.initOwned(alloc, "docs", "{\"term\":{\"team\":\"eng\"}}"),
    };
    defer row_filter[0].deinit(alloc);
    var created = try manager.createApiKey("alice", "ci", &initial, &row_filter, null);
    defer created.deinit(alloc);

    const listed = try manager.listApiKeys("alice");
    defer {
        for (listed) |*entry| entry.deinit(alloc);
        alloc.free(listed);
    }
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqualStrings("ci", listed[0].name);

    var validated = try manager.validateApiKey(created.key.key_id, created.key_secret);
    defer validated.deinit(alloc);
    try std.testing.expectEqualStrings("alice", validated.username);
    try std.testing.expectEqualStrings("{}", validated.metadata_json);
    try std.testing.expectEqual(@as(usize, 1), validated.permissions.len);
    try std.testing.expectEqual(@as(usize, 1), validated.row_filter.len);
    try std.testing.expect(std.mem.indexOf(u8, validated.row_filter[0].filter, "\"tenant_id\":\"acme\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, validated.row_filter[0].filter, "\"team\":\"eng\"") != null);
    try std.testing.expectError(error.ApiKeyInvalid, manager.validateApiKey(created.key.key_id, "bad"));

    var escalated = [_]Permission{
        try Permission.initOwned(alloc, .table, "docs", .admin),
    };
    defer escalated[0].deinit(alloc);
    try std.testing.expectError(error.PrivilegeEscalation, manager.createApiKey("alice", "admin", &escalated, &.{}, null));
}
