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
    \\p3 = sub, setting, value
    \\p4 = table
    \\p5 = sub, database, setting, value
    \\p6 = sub, database, setting, value
    \\p7 = sub, table, target
    \\p8 = sub, obj, filter
    \\[role_definition]
    \\g = _, _
    \\[matchers]
    \\m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
;

const sql_role_catalog_subject = "__antfly_sql_role_catalog__";
const sql_row_security_policy_subject_prefix = "__antfly_sql_rls_policy__:";
const sql_row_security_no_targets_subject = "__antfly_sql_rls_no_targets__";
const global_runtime_role_setting_database = "*";

pub const ResourceType = enum {
    database,
    namespace,
    table,
    tablespace,
    user,
    @"*",

    pub fn fromSlice(raw: []const u8) !ResourceType {
        if (std.mem.eql(u8, raw, "database")) return .database;
        if (std.mem.eql(u8, raw, "namespace")) return .namespace;
        if (std.mem.eql(u8, raw, "table")) return .table;
        if (std.mem.eql(u8, raw, "tablespace")) return .tablespace;
        if (std.mem.eql(u8, raw, "user")) return .user;
        if (std.mem.eql(u8, raw, "*")) return .@"*";
        return error.InvalidResourceType;
    }

    pub fn slice(self: ResourceType) []const u8 {
        return switch (self) {
            .database => "database",
            .namespace => "namespace",
            .table => "table",
            .tablespace => "tablespace",
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

pub const PermissionChangeKind = enum {
    grant,
    revoke,
};

pub const PermissionChange = struct {
    subject: []const u8,
    permission: Permission,
    kind: PermissionChangeKind,
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

pub const RoleSetting = struct {
    name: []u8,
    value: []u8,

    pub fn initOwned(alloc: Allocator, name: []const u8, value: []const u8) !RoleSetting {
        return .{
            .name = try alloc.dupe(u8, name),
            .value = try alloc.dupe(u8, value),
        };
    }

    pub fn deinit(self: *RoleSetting, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const RuntimeRoleSetting = struct {
    database_name: ?[]u8 = null,
    name: []u8,
    value: []u8,

    pub fn initOwned(alloc: Allocator, database_name: ?[]const u8, name: []const u8, value: []const u8) !RuntimeRoleSetting {
        return .{
            .database_name = if (database_name) |db| try alloc.dupe(u8, db) else null,
            .name = try alloc.dupe(u8, name),
            .value = try alloc.dupe(u8, value),
        };
    }

    pub fn deinit(self: *RuntimeRoleSetting, alloc: Allocator) void {
        if (self.database_name) |database_name| alloc.free(database_name);
        alloc.free(self.name);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub fn validateRoleSettingName(name: []const u8) !void {
    if (!std.mem.startsWith(u8, name, "app.") or name.len == "app.".len) return error.UnsupportedRoleSetting;
    var segments = std.mem.splitScalar(u8, name, '.');
    var seen: usize = 0;
    while (segments.next()) |segment| : (seen += 1) {
        if (segment.len == 0) return error.UnsupportedRoleSetting;
    }
    if (seen < 2) return error.UnsupportedRoleSetting;
}

pub fn validateRoleSettingValue(value: []const u8) !void {
    if (value.len == 0) return error.InvalidRoleSetting;
}

pub fn validateRoleSettingDatabaseName(database_name: []const u8) !void {
    if (database_name.len == 0 or std.mem.eql(u8, database_name, global_runtime_role_setting_database)) return error.InvalidRoleSetting;
}

pub fn validateRuntimeRoleSettingName(name: []const u8) !void {
    if (std.mem.eql(u8, name, "statement_timeout")) return;
    if (std.mem.eql(u8, name, "timezone")) return;
    if (std.mem.eql(u8, name, "search_path")) return;
    return error.UnsupportedRoleSetting;
}

pub fn validateRuntimeRoleSettingValue(name: []const u8, value: []const u8) !void {
    if (value.len == 0) return error.InvalidRoleSetting;
    if (std.mem.eql(u8, name, "statement_timeout")) return try validateStatementTimeoutSettingValue(value);
    if (std.mem.eql(u8, name, "search_path")) return try validateSearchPathSettingValue(value);
    if (std.mem.eql(u8, name, "timezone")) return try validateTimezoneSettingValue(value);
    return error.UnsupportedRoleSetting;
}

fn validateStatementTimeoutSettingValue(value: []const u8) !void {
    var digit_count: usize = 0;
    while (digit_count < value.len and std.ascii.isDigit(value[digit_count])) : (digit_count += 1) {}
    if (digit_count == 0) return error.InvalidRoleSetting;
    _ = std.fmt.parseUnsigned(u64, value[0..digit_count], 10) catch return error.InvalidRoleSetting;
    const unit = value[digit_count..];
    if (unit.len == 0) return;
    if (std.mem.eql(u8, unit, "us")) return;
    if (std.mem.eql(u8, unit, "ms")) return;
    if (std.mem.eql(u8, unit, "s")) return;
    if (std.mem.eql(u8, unit, "min")) return;
    if (std.mem.eql(u8, unit, "h")) return;
    return error.InvalidRoleSetting;
}

fn validateSearchPathSettingValue(value: []const u8) !void {
    var iter = std.mem.splitScalar(u8, value, ',');
    var count: usize = 0;
    while (iter.next()) |part| : (count += 1) {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (trimmed.len == 0) return error.InvalidRoleSetting;
        const normalized = if (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"')
            trimmed[1 .. trimmed.len - 1]
        else
            trimmed;
        if (normalized.len == 0) return error.InvalidRoleSetting;
        for (normalized) |c| {
            if (std.ascii.isAlphanumeric(c) or c == '_' or c == '$') continue;
            return error.InvalidRoleSetting;
        }
    }
    if (count == 0) return error.InvalidRoleSetting;
}

fn validateTimezoneSettingValue(value: []const u8) !void {
    for (value) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '_' or c == '/' or c == '+' or c == '-' or c == ':' or c == '.') continue;
        return error.InvalidRoleSetting;
    }
}

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
    role_settings: []RoleSetting = &.{},
    role_runtime_settings: []RoleSetting = &.{},

    pub fn deinit(self: *ValidatedApiKey, alloc: Allocator) void {
        alloc.free(self.username);
        for (self.permissions) |*perm| perm.deinit(alloc);
        alloc.free(self.permissions);
        for (self.row_filter) |*entry| entry.deinit(alloc);
        alloc.free(self.row_filter);
        if (self.metadata_json.len > 0) alloc.free(self.metadata_json);
        freeOwnedStrings(alloc, self.roles);
        for (self.role_settings) |*setting| setting.deinit(alloc);
        if (self.role_settings.len > 0) alloc.free(self.role_settings);
        for (self.role_runtime_settings) |*setting| setting.deinit(alloc);
        if (self.role_runtime_settings.len > 0) alloc.free(self.role_runtime_settings);
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
        _ = try self.enforcer.removeFilteredNamedPolicy("p3", 0, &.{username});
        _ = try self.enforcer.removeFilteredNamedPolicy("p5", 0, &.{username});
        _ = try self.enforcer.removeFilteredNamedPolicy("p6", 0, &.{username});
        _ = try self.enforcer.removeFilteredNamedPolicy("p7", 2, &.{username});
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

    pub fn hasUser(self: *const UserManager, username: []const u8) bool {
        return self.users.contains(username);
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

    pub fn subjectHasPermissionExact(self: *const UserManager, subject: []const u8, permission: Permission) !bool {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p", 0, &.{
            subject,
            permission.resource_type.slice(),
            permission.resource,
            permission.type.slice(),
        });
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        return rules.len > 0;
    }

    pub fn removePermissionFromSubjectExact(self: *UserManager, subject: []const u8, permission: Permission) !void {
        _ = try self.enforcer.removeFilteredPolicy(0, &.{
            subject,
            permission.resource_type.slice(),
            permission.resource,
            permission.type.slice(),
        });
    }

    pub fn applyPermissionChangesAtomically(self: *UserManager, changes: []const PermissionChange) !void {
        if (changes.len == 0) return;
        const existed_before = try self.alloc.alloc(bool, changes.len);
        defer self.alloc.free(existed_before);
        for (changes, 0..) |change, i| {
            existed_before[i] = try self.subjectHasPermissionExact(change.subject, change.permission);
        }

        var applied: usize = 0;
        errdefer {
            var i = applied;
            while (i > 0) {
                i -= 1;
                const change = changes[i];
                switch (change.kind) {
                    .grant => {
                        if (!existed_before[i]) self.removePermissionFromSubjectExact(change.subject, change.permission) catch {};
                    },
                    .revoke => {
                        if (existed_before[i]) self.addPermissionToSubject(change.subject, change.permission) catch {};
                    },
                }
            }
        }

        for (changes) |change| {
            switch (change.kind) {
                .grant => try self.addPermissionToSubject(change.subject, change.permission),
                .revoke => try self.removePermissionFromSubjectExact(change.subject, change.permission),
            }
            applied += 1;
        }
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

    pub fn createRoleSubject(self: *UserManager, role_subject: []const u8) !void {
        if (role_subject.len == 0) return error.InvalidRole;
        if (try self.roleSubjectExists(role_subject)) return error.RoleExists;
        _ = try self.enforcer.addNamedPolicy("g", &.{ sql_role_catalog_subject, role_subject });
    }

    pub fn dropRoleSubject(self: *UserManager, role_subject: []const u8) !void {
        if (!(try self.roleSubjectExists(role_subject))) return error.RoleNotFound;
        if (try self.roleSubjectHasDependencies(role_subject)) return error.RoleInUse;
        _ = try self.enforcer.removeFilteredNamedPolicy("g", 0, &.{ sql_role_catalog_subject, role_subject });
        _ = try self.enforcer.removeFilteredNamedPolicy("p3", 0, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("p5", 0, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("p6", 0, &.{role_subject});
    }

    pub fn dropRoleSubjectCascade(self: *UserManager, role_subject: []const u8) !void {
        if (!(try self.roleSubjectExists(role_subject))) return error.RoleNotFound;
        try self.removeSqlRowSecurityPolicyTargetCascade(role_subject);
        _ = try self.enforcer.removeFilteredPolicy(0, &.{role_subject});
        _ = try self.enforcer.removeFilteredGroupingPolicy(0, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("g", 0, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("g", 1, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("p2", 0, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("p8", 0, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("p3", 0, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("p5", 0, &.{role_subject});
        _ = try self.enforcer.removeFilteredNamedPolicy("p6", 0, &.{role_subject});
    }

    const SqlRowSecurityPolicyTargetRef = struct {
        policy_subject: []u8,
        table: []u8,

        fn deinit(self: *@This(), alloc: Allocator) void {
            alloc.free(self.policy_subject);
            alloc.free(self.table);
            self.* = undefined;
        }
    };

    fn removeSqlRowSecurityPolicyTargetCascade(self: *UserManager, role_subject: []const u8) !void {
        const target_rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p7", 2, &.{role_subject});
        defer {
            for (target_rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(target_rules);
        }

        var affected = std.ArrayList(SqlRowSecurityPolicyTargetRef).empty;
        defer {
            for (affected.items) |*entry| entry.deinit(self.alloc);
            affected.deinit(self.alloc);
        }
        for (target_rules) |rule| {
            if (rule.fields.len < 3) continue;
            try affected.append(self.alloc, .{
                .policy_subject = try self.alloc.dupe(u8, rule.fields[0]),
                .table = try self.alloc.dupe(u8, rule.fields[1]),
            });
        }

        _ = try self.enforcer.removeFilteredNamedPolicy("p7", 2, &.{role_subject});
        for (affected.items) |entry| {
            const remaining = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p7", 0, &.{ entry.policy_subject, entry.table });
            defer {
                for (remaining) |*rule| rule.deinit(self.alloc);
                self.alloc.free(remaining);
            }
            if (remaining.len == 0) {
                _ = try self.enforcer.addNamedPolicy("p7", &.{ entry.policy_subject, entry.table, sql_row_security_no_targets_subject });
            }
        }
    }

    pub fn roleSubjectHasDependencies(self: *const UserManager, role_subject: []const u8) !bool {
        if (try self.hasFilteredPolicy("p", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("p2", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("p8", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("p7", 2, role_subject)) return true;
        if (try self.hasFilteredPolicy("g", 0, role_subject)) return true;

        const inbound_rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "g", 1, &.{role_subject});
        defer {
            for (inbound_rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(inbound_rules);
        }
        for (inbound_rules) |rule| {
            if (rule.fields.len < 2) continue;
            if (std.mem.eql(u8, rule.fields[0], sql_role_catalog_subject)) continue;
            return true;
        }
        return false;
    }

    pub fn roleSubjectExists(self: *const UserManager, role_subject: []const u8) !bool {
        if (role_subject.len == 0) return false;
        if (self.users.contains(role_subject)) return true;
        if (try self.hasFilteredPolicy("p", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("p2", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("p8", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("p5", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("p6", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("p7", 2, role_subject)) return true;
        if (try self.hasFilteredPolicy("g", 0, role_subject)) return true;
        if (try self.hasFilteredPolicy("g", 1, role_subject)) return true;
        return false;
    }

    fn hasFilteredPolicy(self: *const UserManager, ptype: []const u8, field_index: usize, value: []const u8) !bool {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, ptype, field_index, &.{value});
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        return rules.len > 0;
    }

    pub fn setRoleSetting(self: *UserManager, role_subject: []const u8, setting_name: []const u8, setting_value: []const u8) !void {
        if (!(try self.roleSubjectExists(role_subject))) return error.RoleNotFound;
        try validateRoleSettingName(setting_name);
        try validateRoleSettingValue(setting_value);

        const existing_rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p3", 0, &.{ role_subject, setting_name });
        defer {
            for (existing_rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(existing_rules);
        }

        var setting_present = false;
        for (existing_rules) |rule| {
            if (rule.fields.len >= 3 and std.mem.eql(u8, rule.fields[2], setting_value)) {
                setting_present = true;
                break;
            }
        }
        const inserted_new = if (setting_present)
            false
        else
            try self.enforcer.addNamedPolicy("p3", &.{ role_subject, setting_name, setting_value });
        errdefer if (inserted_new) {
            _ = self.enforcer.removeFilteredNamedPolicy("p3", 0, &.{ role_subject, setting_name, setting_value }) catch {};
        };

        var removed_old_values = std.ArrayList([]const u8).empty;
        defer removed_old_values.deinit(self.alloc);
        try removed_old_values.ensureTotalCapacity(self.alloc, existing_rules.len);
        errdefer {
            for (removed_old_values.items) |old_value| {
                _ = self.enforcer.addNamedPolicy("p3", &.{ role_subject, setting_name, old_value }) catch {};
            }
        }

        for (existing_rules) |rule| {
            if (rule.fields.len < 3 or std.mem.eql(u8, rule.fields[2], setting_value)) continue;
            if (try self.enforcer.removeFilteredNamedPolicy("p3", 0, &.{ role_subject, setting_name, rule.fields[2] })) {
                removed_old_values.appendAssumeCapacity(rule.fields[2]);
            }
        }
    }

    pub fn getRoleSetting(self: *const UserManager, role_subject: []const u8, setting_name: []const u8) ![]u8 {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p3", 0, &.{ role_subject, setting_name });
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        if (rules.len == 0 or rules[0].fields.len < 3) return error.RoleSettingNotFound;
        return try self.alloc.dupe(u8, rules[0].fields[2]);
    }

    pub fn removeRoleSetting(self: *UserManager, role_subject: []const u8, setting_name: []const u8) !void {
        const removed = try self.enforcer.removeFilteredNamedPolicy("p3", 0, &.{ role_subject, setting_name });
        if (!removed) return error.RoleSettingNotFound;
    }

    pub fn setRoleDatabaseSetting(
        self: *UserManager,
        role_subject: []const u8,
        database_name: []const u8,
        setting_name: []const u8,
        setting_value: []const u8,
    ) !void {
        if (!(try self.roleSubjectExists(role_subject))) return error.RoleNotFound;
        try validateRoleSettingDatabaseName(database_name);
        try validateRoleSettingName(setting_name);
        try validateRoleSettingValue(setting_value);

        const existing_rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p6", 0, &.{ role_subject, database_name, setting_name });
        defer {
            for (existing_rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(existing_rules);
        }

        var setting_present = false;
        for (existing_rules) |rule| {
            if (rule.fields.len >= 4 and std.mem.eql(u8, rule.fields[3], setting_value)) {
                setting_present = true;
                break;
            }
        }
        const inserted_new = if (setting_present)
            false
        else
            try self.enforcer.addNamedPolicy("p6", &.{ role_subject, database_name, setting_name, setting_value });
        errdefer if (inserted_new) {
            _ = self.enforcer.removeFilteredNamedPolicy("p6", 0, &.{ role_subject, database_name, setting_name, setting_value }) catch {};
        };

        var removed_old_values = std.ArrayList([]const u8).empty;
        defer removed_old_values.deinit(self.alloc);
        try removed_old_values.ensureTotalCapacity(self.alloc, existing_rules.len);
        errdefer {
            for (removed_old_values.items) |old_value| {
                _ = self.enforcer.addNamedPolicy("p6", &.{ role_subject, database_name, setting_name, old_value }) catch {};
            }
        }

        for (existing_rules) |rule| {
            if (rule.fields.len < 4 or std.mem.eql(u8, rule.fields[3], setting_value)) continue;
            if (try self.enforcer.removeFilteredNamedPolicy("p6", 0, &.{ role_subject, database_name, setting_name, rule.fields[3] })) {
                removed_old_values.appendAssumeCapacity(rule.fields[3]);
            }
        }
    }

    pub fn getRoleDatabaseSetting(self: *const UserManager, role_subject: []const u8, database_name: []const u8, setting_name: []const u8) ![]u8 {
        try validateRoleSettingDatabaseName(database_name);
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p6", 0, &.{ role_subject, database_name, setting_name });
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        if (rules.len == 0 or rules[0].fields.len < 4) return error.RoleSettingNotFound;
        return try self.alloc.dupe(u8, rules[0].fields[3]);
    }

    pub fn removeRoleDatabaseSetting(self: *UserManager, role_subject: []const u8, database_name: []const u8, setting_name: []const u8) !void {
        try validateRoleSettingDatabaseName(database_name);
        const removed = try self.enforcer.removeFilteredNamedPolicy("p6", 0, &.{ role_subject, database_name, setting_name });
        if (!removed) return error.RoleSettingNotFound;
    }

    pub fn setRoleRuntimeSetting(
        self: *UserManager,
        role_subject: []const u8,
        database_name: ?[]const u8,
        setting_name: []const u8,
        setting_value: []const u8,
    ) !void {
        if (!(try self.roleSubjectExists(role_subject))) return error.RoleNotFound;
        if (database_name) |db| if (db.len == 0 or std.mem.eql(u8, db, global_runtime_role_setting_database)) return error.InvalidRoleSetting;
        try validateRuntimeRoleSettingName(setting_name);
        try validateRuntimeRoleSettingValue(setting_name, setting_value);
        const database_key = runtimeRoleSettingDatabaseKey(database_name);

        const existing_rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p5", 0, &.{ role_subject, database_key, setting_name });
        defer {
            for (existing_rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(existing_rules);
        }

        var setting_present = false;
        for (existing_rules) |rule| {
            if (rule.fields.len >= 4 and std.mem.eql(u8, rule.fields[3], setting_value)) {
                setting_present = true;
                break;
            }
        }
        const inserted_new = if (setting_present)
            false
        else
            try self.enforcer.addNamedPolicy("p5", &.{ role_subject, database_key, setting_name, setting_value });
        errdefer if (inserted_new) {
            _ = self.enforcer.removeFilteredNamedPolicy("p5", 0, &.{ role_subject, database_key, setting_name, setting_value }) catch {};
        };

        var removed_old_values = std.ArrayList([]const u8).empty;
        defer removed_old_values.deinit(self.alloc);
        try removed_old_values.ensureTotalCapacity(self.alloc, existing_rules.len);
        errdefer {
            for (removed_old_values.items) |old_value| {
                _ = self.enforcer.addNamedPolicy("p5", &.{ role_subject, database_key, setting_name, old_value }) catch {};
            }
        }

        for (existing_rules) |rule| {
            if (rule.fields.len < 4 or std.mem.eql(u8, rule.fields[3], setting_value)) continue;
            if (try self.enforcer.removeFilteredNamedPolicy("p5", 0, &.{ role_subject, database_key, setting_name, rule.fields[3] })) {
                removed_old_values.appendAssumeCapacity(rule.fields[3]);
            }
        }
    }

    pub fn getRoleRuntimeSetting(self: *const UserManager, role_subject: []const u8, database_name: ?[]const u8, setting_name: []const u8) ![]u8 {
        const database_key = runtimeRoleSettingDatabaseKey(database_name);
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p5", 0, &.{ role_subject, database_key, setting_name });
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        if (rules.len == 0 or rules[0].fields.len < 4) return error.RoleSettingNotFound;
        return try self.alloc.dupe(u8, rules[0].fields[3]);
    }

    pub fn removeRoleRuntimeSetting(self: *UserManager, role_subject: []const u8, database_name: ?[]const u8, setting_name: []const u8) !void {
        const database_key = runtimeRoleSettingDatabaseKey(database_name);
        const removed = try self.enforcer.removeFilteredNamedPolicy("p5", 0, &.{ role_subject, database_key, setting_name });
        if (!removed) return error.RoleSettingNotFound;
    }

    pub fn listRoleRuntimeSettingsForSubject(self: *const UserManager, subject: []const u8, database_name: ?[]const u8) ![]RoleSetting {
        const database_key = runtimeRoleSettingDatabaseKey(database_name);
        if (database_name) |db| try validateRoleSettingDatabaseName(db);
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p5", 0, &.{ subject, database_key });
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        var out = std.ArrayList(RoleSetting).empty;
        errdefer {
            for (out.items) |*setting| setting.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        for (rules) |rule| {
            if (rule.fields.len < 4) continue;
            try out.append(self.alloc, try RoleSetting.initOwned(self.alloc, rule.fields[2], rule.fields[3]));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn listRoleSettingsForSubject(self: *const UserManager, subject: []const u8) ![]RoleSetting {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p3", 0, &.{subject});
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        var out = std.ArrayList(RoleSetting).empty;
        errdefer {
            for (out.items) |*setting| setting.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        for (rules) |rule| {
            if (rule.fields.len < 3) continue;
            try out.append(self.alloc, try RoleSetting.initOwned(self.alloc, rule.fields[1], rule.fields[2]));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn listRoleDatabaseSettingsForSubject(self: *const UserManager, subject: []const u8, database_name: []const u8) ![]RoleSetting {
        try validateRoleSettingDatabaseName(database_name);
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p6", 0, &.{ subject, database_name });
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }
        var out = std.ArrayList(RoleSetting).empty;
        errdefer {
            for (out.items) |*setting| setting.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        for (rules) |rule| {
            if (rule.fields.len < 4) continue;
            try out.append(self.alloc, try RoleSetting.initOwned(self.alloc, rule.fields[2], rule.fields[3]));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    fn runtimeRoleSettingDatabaseKey(database_name: ?[]const u8) []const u8 {
        return database_name orelse global_runtime_role_setting_database;
    }

    pub fn getEffectiveRoleSettings(self: *const UserManager, username: []const u8) ![]RoleSetting {
        return try self.getEffectiveRoleSettingsForDatabase(username, null);
    }

    pub fn getEffectiveRoleSettingsForDatabase(self: *const UserManager, username: []const u8, database_name: ?[]const u8) ![]RoleSetting {
        if (!self.users.contains(username)) return error.UserNotFound;
        if (database_name) |db| try validateRoleSettingDatabaseName(db);
        const roles = try self.getRolesForUser(username);
        defer freeOwnedStrings(self.alloc, roles);
        const direct_settings = try self.listRoleSettingsForSubject(username);
        defer {
            for (direct_settings) |*setting| setting.deinit(self.alloc);
            self.alloc.free(direct_settings);
        }
        const direct_database_settings: []RoleSetting = if (database_name) |db|
            try self.listRoleDatabaseSettingsForSubject(username, db)
        else
            &.{};
        defer {
            for (direct_database_settings) |*setting| setting.deinit(self.alloc);
            if (direct_database_settings.len > 0) self.alloc.free(direct_database_settings);
        }

        var merged = std.StringArrayHashMapUnmanaged([]u8){};
        defer {
            var it = merged.iterator();
            while (it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            merged.deinit(self.alloc);
        }

        for (roles) |role| try self.mergeInheritedRoleSettingsForSubject(&merged, role, direct_settings, direct_database_settings, database_name);
        for (direct_settings) |setting| try putRoleSetting(&merged, self.alloc, setting.name, setting.value, .replace);
        for (direct_database_settings) |setting| try putRoleSetting(&merged, self.alloc, setting.name, setting.value, .replace);

        var out = std.ArrayList(RoleSetting).empty;
        errdefer {
            for (out.items) |*setting| setting.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        var it = merged.iterator();
        while (it.next()) |entry| {
            try out.append(self.alloc, try RoleSetting.initOwned(self.alloc, entry.key_ptr.*, entry.value_ptr.*));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn getEffectiveRoleRuntimeSettings(self: *const UserManager, username: []const u8) ![]RoleSetting {
        return try self.getEffectiveRoleRuntimeSettingsForDatabase(username, null);
    }

    pub fn getEffectiveRoleRuntimeSettingsForDatabase(self: *const UserManager, username: []const u8, database_name: ?[]const u8) ![]RoleSetting {
        if (!self.users.contains(username)) return error.UserNotFound;
        if (database_name) |db| try validateRoleSettingDatabaseName(db);
        const roles = try self.getRolesForUser(username);
        defer freeOwnedStrings(self.alloc, roles);
        const direct_settings = try self.listRoleRuntimeSettingsForSubject(username, null);
        defer {
            for (direct_settings) |*setting| setting.deinit(self.alloc);
            self.alloc.free(direct_settings);
        }
        const direct_database_settings: []RoleSetting = if (database_name) |db|
            try self.listRoleRuntimeSettingsForSubject(username, db)
        else
            &.{};
        defer {
            for (direct_database_settings) |*setting| setting.deinit(self.alloc);
            if (direct_database_settings.len > 0) self.alloc.free(direct_database_settings);
        }

        var merged = std.StringArrayHashMapUnmanaged([]u8){};
        defer {
            var it = merged.iterator();
            while (it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            merged.deinit(self.alloc);
        }

        for (roles) |role| try self.mergeInheritedRuntimeRoleSettingsForSubject(&merged, role, direct_settings, direct_database_settings, database_name);
        for (direct_settings) |setting| try putRoleSetting(&merged, self.alloc, setting.name, setting.value, .replace);
        for (direct_database_settings) |setting| try putRoleSetting(&merged, self.alloc, setting.name, setting.value, .replace);

        var out = std.ArrayList(RoleSetting).empty;
        errdefer {
            for (out.items) |*setting| setting.deinit(self.alloc);
            out.deinit(self.alloc);
        }
        var it = merged.iterator();
        while (it.next()) |entry| {
            try out.append(self.alloc, try RoleSetting.initOwned(self.alloc, entry.key_ptr.*, entry.value_ptr.*));
        }
        return try out.toOwnedSlice(self.alloc);
    }

    const RoleSettingMergeMode = enum {
        require_same_value,
        replace,
    };

    fn mergeInheritedRoleSettingsForSubject(
        self: *const UserManager,
        merged: *std.StringArrayHashMapUnmanaged([]u8),
        subject: []const u8,
        direct_settings: []const RoleSetting,
        direct_database_settings: []const RoleSetting,
        database_name: ?[]const u8,
    ) !void {
        var subject_settings = std.StringArrayHashMapUnmanaged([]u8){};
        defer {
            var it = subject_settings.iterator();
            while (it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            subject_settings.deinit(self.alloc);
        }

        const settings = try self.listRoleSettingsForSubject(subject);
        defer {
            for (settings) |*setting| setting.deinit(self.alloc);
            self.alloc.free(settings);
        }
        for (settings) |setting| {
            if (roleSettingsContainName(direct_settings, setting.name) or roleSettingsContainName(direct_database_settings, setting.name)) continue;
            try putRoleSetting(&subject_settings, self.alloc, setting.name, setting.value, .replace);
        }
        if (database_name) |db| {
            const database_settings = try self.listRoleDatabaseSettingsForSubject(subject, db);
            defer {
                for (database_settings) |*setting| setting.deinit(self.alloc);
                self.alloc.free(database_settings);
            }
            for (database_settings) |setting| {
                if (roleSettingsContainName(direct_settings, setting.name) or roleSettingsContainName(direct_database_settings, setting.name)) continue;
                try putRoleSetting(&subject_settings, self.alloc, setting.name, setting.value, .replace);
            }
        }

        var it = subject_settings.iterator();
        while (it.next()) |entry| {
            try putRoleSetting(merged, self.alloc, entry.key_ptr.*, entry.value_ptr.*, .require_same_value);
        }
    }

    fn mergeInheritedRuntimeRoleSettingsForSubject(
        self: *const UserManager,
        merged: *std.StringArrayHashMapUnmanaged([]u8),
        subject: []const u8,
        direct_settings: []const RoleSetting,
        direct_database_settings: []const RoleSetting,
        database_name: ?[]const u8,
    ) !void {
        var subject_settings = std.StringArrayHashMapUnmanaged([]u8){};
        defer {
            var it = subject_settings.iterator();
            while (it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            subject_settings.deinit(self.alloc);
        }

        const settings = try self.listRoleRuntimeSettingsForSubject(subject, null);
        defer {
            for (settings) |*setting| setting.deinit(self.alloc);
            self.alloc.free(settings);
        }
        for (settings) |setting| {
            if (roleSettingsContainName(direct_settings, setting.name) or roleSettingsContainName(direct_database_settings, setting.name)) continue;
            try putRoleSetting(&subject_settings, self.alloc, setting.name, setting.value, .replace);
        }
        if (database_name) |db| {
            const database_settings = try self.listRoleRuntimeSettingsForSubject(subject, db);
            defer {
                for (database_settings) |*setting| setting.deinit(self.alloc);
                self.alloc.free(database_settings);
            }
            for (database_settings) |setting| {
                if (roleSettingsContainName(direct_settings, setting.name) or roleSettingsContainName(direct_database_settings, setting.name)) continue;
                try putRoleSetting(&subject_settings, self.alloc, setting.name, setting.value, .replace);
            }
        }

        var it = subject_settings.iterator();
        while (it.next()) |entry| {
            try putRoleSetting(merged, self.alloc, entry.key_ptr.*, entry.value_ptr.*, .require_same_value);
        }
    }

    fn roleSettingsContainName(settings: []const RoleSetting, name: []const u8) bool {
        for (settings) |setting| {
            if (std.mem.eql(u8, setting.name, name)) return true;
        }
        return false;
    }

    fn putRoleSetting(
        merged: *std.StringArrayHashMapUnmanaged([]u8),
        alloc: Allocator,
        name: []const u8,
        value: []const u8,
        mode: RoleSettingMergeMode,
    ) !void {
        if (merged.get(name)) |existing| {
            switch (mode) {
                .require_same_value => {
                    if (!std.mem.eql(u8, existing, value)) return error.RoleSettingConflict;
                    return;
                },
                .replace => {},
            }
        }
        if (merged.fetchOrderedRemove(name)) |removed| {
            alloc.free(removed.key);
            alloc.free(removed.value);
        }
        try merged.put(
            alloc,
            try alloc.dupe(u8, name),
            try alloc.dupe(u8, value),
        );
    }

    pub fn enableSqlRowSecurity(self: *UserManager, table: []const u8) !void {
        if (table.len == 0) return error.InvalidRowFilter;
        if (try self.sqlRowSecurityEnabled(table)) return;
        _ = try self.enforcer.addNamedPolicy("p4", &.{table});
    }

    pub fn disableSqlRowSecurity(self: *UserManager, table: []const u8) !void {
        if (table.len == 0) return error.InvalidRowFilter;
        if (!(try self.sqlRowSecurityEnabled(table))) return;
        _ = try self.enforcer.removeFilteredNamedPolicy("p4", 0, &.{table});
    }

    pub fn sqlRowSecurityEnabled(self: *const UserManager, table: []const u8) !bool {
        if (table.len == 0) return false;
        return try self.hasFilteredPolicy("p4", 0, table);
    }

    pub fn createSqlRowSecurityPolicy(self: *UserManager, policy_name: []const u8, table: []const u8, filter_json: []const u8) !void {
        try self.createSqlRowSecurityPolicyWithTargets(policy_name, table, filter_json, &.{});
    }

    pub fn createSqlRowSecurityPolicyWithTargets(self: *UserManager, policy_name: []const u8, table: []const u8, filter_json: []const u8, role_targets: []const []const u8) !void {
        try self.createSqlRowSecurityPolicyWithTargetsAndCheck(policy_name, table, filter_json, filter_json, role_targets);
    }

    pub fn createSqlRowSecurityPolicyWithTargetsAndCheck(self: *UserManager, policy_name: []const u8, table: []const u8, filter_json: []const u8, check_filter_json: []const u8, role_targets: []const []const u8) !void {
        const subject = try sqlRowSecurityPolicySubjectAlloc(self.alloc, policy_name);
        defer self.alloc.free(subject);
        if (self.getSubjectRowFilter(subject, table)) |existing| {
            self.alloc.free(existing);
            return error.PolicyExists;
        } else |err| switch (err) {
            error.RowFilterNotFound => {},
            else => return err,
        }
        try self.setSubjectRowFilter(subject, table, filter_json);
        errdefer self.removeSubjectRowFilter(subject, table) catch {};
        try self.setSubjectSqlRowSecurityCheckFilter(subject, table, check_filter_json);
        try self.replaceSqlRowSecurityPolicyTargets(subject, table, role_targets);
    }

    pub fn replaceSqlRowSecurityPolicy(self: *UserManager, policy_name: []const u8, table: []const u8, filter_json: []const u8) !void {
        try self.replaceSqlRowSecurityPolicyWithTargets(policy_name, table, filter_json, &.{});
    }

    pub fn replaceSqlRowSecurityPolicyWithTargets(self: *UserManager, policy_name: []const u8, table: []const u8, filter_json: []const u8, role_targets: []const []const u8) !void {
        try self.replaceSqlRowSecurityPolicyWithTargetsAndCheck(policy_name, table, filter_json, filter_json, role_targets);
    }

    pub fn replaceSqlRowSecurityPolicyWithTargetsAndCheck(self: *UserManager, policy_name: []const u8, table: []const u8, filter_json: []const u8, check_filter_json: []const u8, role_targets: []const []const u8) !void {
        const subject = try sqlRowSecurityPolicySubjectAlloc(self.alloc, policy_name);
        defer self.alloc.free(subject);
        const existing = try self.getSubjectRowFilter(subject, table);
        self.alloc.free(existing);
        try self.setSubjectRowFilter(subject, table, filter_json);
        try self.setSubjectSqlRowSecurityCheckFilter(subject, table, check_filter_json);
        try self.replaceSqlRowSecurityPolicyTargets(subject, table, role_targets);
    }

    pub fn dropSqlRowSecurityPolicy(self: *UserManager, policy_name: []const u8, table: []const u8) !void {
        const subject = try sqlRowSecurityPolicySubjectAlloc(self.alloc, policy_name);
        defer self.alloc.free(subject);
        try self.removeSubjectRowFilter(subject, table);
        _ = try self.enforcer.removeFilteredNamedPolicy("p8", 0, &.{ subject, table });
        _ = try self.enforcer.removeFilteredNamedPolicy("p7", 0, &.{ subject, table });
    }

    pub fn getSqlRowSecurityPolicy(self: *const UserManager, policy_name: []const u8, table: []const u8) ![]u8 {
        const subject = try sqlRowSecurityPolicySubjectAlloc(self.alloc, policy_name);
        defer self.alloc.free(subject);
        return try self.getSubjectRowFilter(subject, table);
    }

    pub fn getSqlRowSecurityPolicyCheck(self: *const UserManager, policy_name: []const u8, table: []const u8) ![]u8 {
        const subject = try sqlRowSecurityPolicySubjectAlloc(self.alloc, policy_name);
        defer self.alloc.free(subject);
        return try self.getSubjectSqlRowSecurityCheckFilter(subject, table);
    }

    pub fn getSqlRowSecurityPolicyTargets(self: *const UserManager, policy_name: []const u8, table: []const u8) ![]const []const u8 {
        const subject = try sqlRowSecurityPolicySubjectAlloc(self.alloc, policy_name);
        defer self.alloc.free(subject);
        const existing = try self.getSubjectRowFilter(subject, table);
        self.alloc.free(existing);
        const target_rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p7", 0, &.{ subject, table });
        defer {
            for (target_rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(target_rules);
        }
        var targets = std.ArrayList([]const u8).empty;
        errdefer {
            for (targets.items) |target| self.alloc.free(@constCast(target));
            targets.deinit(self.alloc);
        }
        for (target_rules) |rule| {
            if (rule.fields.len < 3) continue;
            const target = try self.alloc.dupe(u8, rule.fields[2]);
            errdefer self.alloc.free(target);
            try targets.append(self.alloc, target);
        }
        return try targets.toOwnedSlice(self.alloc);
    }

    fn replaceSqlRowSecurityPolicyTargets(self: *UserManager, policy_subject: []const u8, table: []const u8, role_targets: []const []const u8) !void {
        _ = try self.enforcer.removeFilteredNamedPolicy("p7", 0, &.{ policy_subject, table });
        for (role_targets) |target| {
            if (target.len == 0) return error.InvalidRole;
            _ = try self.enforcer.addNamedPolicy("p7", &.{ policy_subject, table, target });
        }
    }

    fn mergeSqlRowSecurityPolicyFilters(self: *const UserManager, username: []const u8, roles: []const []const u8, merged: *std.StringArrayHashMapUnmanaged([]u8)) !void {
        return try self.mergeSqlRowSecurityPolicyFiltersFromPolicy("p2", username, roles, merged);
    }

    fn mergeSqlRowSecurityPolicyCheckFilters(self: *const UserManager, username: []const u8, roles: []const []const u8, merged: *std.StringArrayHashMapUnmanaged([]u8)) !void {
        return try self.mergeSqlRowSecurityPolicyFiltersFromPolicy("p8", username, roles, merged);
    }

    fn mergeSqlRowSecurityPolicyFiltersFromPolicy(self: *const UserManager, ptype: []const u8, username: []const u8, roles: []const []const u8, merged: *std.StringArrayHashMapUnmanaged([]u8)) !void {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, ptype, 0, &.{});
        defer {
            for (rules) |*rule| rule.deinit(self.alloc);
            self.alloc.free(rules);
        }

        var sql_filters = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged([]u8)){};
        defer {
            var it = sql_filters.iterator();
            while (it.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                for (entry.value_ptr.items) |filter| self.alloc.free(filter);
                entry.value_ptr.deinit(self.alloc);
            }
            sql_filters.deinit(self.alloc);
        }

        for (rules) |rule| {
            if (rule.fields.len < 3) continue;
            if (!isSqlRowSecurityPolicySubject(rule.fields[0])) continue;
            if (!(try self.sqlRowSecurityEnabled(rule.fields[1]))) continue;
            if (!(try self.sqlRowSecurityPolicyAppliesToUser(rule.fields[0], rule.fields[1], username, roles))) continue;
            const gop = try sql_filters.getOrPut(self.alloc, rule.fields[1]);
            if (!gop.found_existing) {
                gop.key_ptr.* = try self.alloc.dupe(u8, rule.fields[1]);
                gop.value_ptr.* = .empty;
            }
            const filter = try self.alloc.dupe(u8, rule.fields[2]);
            errdefer self.alloc.free(filter);
            try gop.value_ptr.append(self.alloc, filter);
        }

        var it = sql_filters.iterator();
        while (it.next()) |entry| {
            const filter = try sqlRowSecurityPermissiveFilterJsonAlloc(self.alloc, entry.value_ptr.items);
            defer self.alloc.free(filter);
            try mergeRowFilter(self.alloc, merged, entry.key_ptr.*, filter);
        }
    }

    fn sqlRowSecurityPermissiveFilterJsonAlloc(alloc: Allocator, filters: []const []const u8) ![]u8 {
        if (filters.len == 0) return error.InvalidRowFilter;
        if (filters.len == 1) return try alloc.dupe(u8, filters[0]);

        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "{\"disjuncts\":[");
        for (filters, 0..) |filter, i| {
            if (i != 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, filter);
        }
        try out.appendSlice(alloc, "]}");
        return try out.toOwnedSlice(alloc);
    }

    fn sqlRowSecurityPolicyAppliesToUser(self: *const UserManager, policy_subject: []const u8, table: []const u8, username: []const u8, roles: []const []const u8) !bool {
        const targets = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p7", 0, &.{ policy_subject, table });
        defer {
            for (targets) |*rule| rule.deinit(self.alloc);
            self.alloc.free(targets);
        }
        if (targets.len == 0) return true;
        for (targets) |target| {
            if (target.fields.len < 3) continue;
            if (std.mem.eql(u8, target.fields[2], sql_row_security_no_targets_subject)) continue;
            if (std.mem.eql(u8, target.fields[2], username)) return true;
            for (roles) |role| {
                if (std.mem.eql(u8, target.fields[2], role)) return true;
            }
        }
        return false;
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
        try self.collectAuthSubjectsFromPolicy(&subjects, "p8");
        try self.collectAuthSubjectsFromPolicy(&subjects, "p7");
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
            if (std.mem.eql(u8, ptype, "p7") and rule.fields.len >= 3) {
                try putAuthSubject(self.alloc, subjects, rule.fields[2], inferAuthSubjectKind(rule.fields[2]));
            }
        }
    }

    pub fn setSubjectRowFilter(self: *UserManager, subject: []const u8, table: []const u8, filter_json: []const u8) !void {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, filter_json, .{});
        parsed.deinit();
        _ = try self.enforcer.removeFilteredNamedPolicy("p2", 0, &.{ subject, table });
        _ = try self.enforcer.addNamedPolicy("p2", &.{ subject, table, filter_json });
    }

    fn setSubjectSqlRowSecurityCheckFilter(self: *UserManager, subject: []const u8, table: []const u8, filter_json: []const u8) !void {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, filter_json, .{});
        parsed.deinit();
        _ = try self.enforcer.removeFilteredNamedPolicy("p8", 0, &.{ subject, table });
        _ = try self.enforcer.addNamedPolicy("p8", &.{ subject, table, filter_json });
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

    fn getSubjectSqlRowSecurityCheckFilter(self: *const UserManager, subject: []const u8, table: []const u8) ![]u8 {
        const rules = try self.enforcer.getFilteredNamedPolicy(self.alloc, "p8", 0, &.{ subject, table });
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
        return try self.getEffectiveRowFiltersFromSqlPolicy(username, .read);
    }

    pub fn getWriteRowFilters(self: *const UserManager, username: []const u8) ![]RowFilterEntry {
        return try self.getEffectiveRowFiltersFromSqlPolicy(username, .write);
    }

    const SqlRowSecurityFilterMode = enum {
        read,
        write,
    };

    fn getEffectiveRowFiltersFromSqlPolicy(self: *const UserManager, username: []const u8, mode: SqlRowSecurityFilterMode) ![]RowFilterEntry {
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

        switch (mode) {
            .read => try self.mergeSqlRowSecurityPolicyFilters(username, roles, &merged),
            .write => try self.mergeSqlRowSecurityPolicyCheckFilters(username, roles, &merged),
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
            .role_settings = try self.getEffectiveRoleSettings(record.key.username),
            .role_runtime_settings = try self.getEffectiveRoleRuntimeSettings(record.key.username),
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
    try mergeRowFilter(alloc, merged, entry.table, entry.filter);
}

fn mergeRowFilter(
    alloc: Allocator,
    merged: *std.StringArrayHashMapUnmanaged([]u8),
    table: []const u8,
    filter: []const u8,
) !void {
    const gop = try merged.getOrPut(alloc, table);
    if (!gop.found_existing) {
        gop.key_ptr.* = try alloc.dupe(u8, table);
        gop.value_ptr.* = try alloc.dupe(u8, filter);
        return;
    }

    const combined = try std.fmt.allocPrint(
        alloc,
        "{{\"conjuncts\":[{s},{s}]}}",
        .{ gop.value_ptr.*, filter },
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
    if (std.mem.eql(u8, subject, sql_role_catalog_subject)) return;
    if (std.mem.eql(u8, subject, sql_row_security_no_targets_subject)) return;
    if (isSqlRowSecurityPolicySubject(subject)) return;
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

fn sqlRowSecurityPolicySubjectAlloc(alloc: Allocator, policy_name: []const u8) ![]u8 {
    if (policy_name.len == 0) return error.InvalidRole;
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ sql_row_security_policy_subject_prefix, policy_name });
}

fn isSqlRowSecurityPolicySubject(subject: []const u8) bool {
    return std.mem.startsWith(u8, subject, sql_row_security_policy_subject_prefix);
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

test "usermgr role settings are native app settings only and replace durably" {
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

    try manager.createRoleSubject("role:app_writer");
    try std.testing.expectError(error.UnsupportedRoleSetting, manager.setRoleSetting("role:app_writer", "statement_timeout", "5s"));
    try std.testing.expectError(error.UnsupportedRoleSetting, manager.setRoleSetting("role:app_writer", "app.", "acme"));
    try std.testing.expectError(error.UnsupportedRoleSetting, manager.setRoleSetting("role:app_writer", "app..tenant_id", "acme"));

    try manager.setRoleSetting("role:app_writer", "app.tenant_id", "acme");
    try manager.setRoleSetting("role:app_writer", "app.tenant_id", "other");
    const setting = try manager.getRoleSetting("role:app_writer", "app.tenant_id");
    defer alloc.free(setting);
    try std.testing.expectEqualStrings("other", setting);

    const settings = try manager.listRoleSettingsForSubject("role:app_writer");
    defer {
        for (settings) |*entry| entry.deinit(alloc);
        alloc.free(settings);
    }
    try std.testing.expectEqual(@as(usize, 1), settings.len);
    try std.testing.expectEqualStrings("app.tenant_id", settings[0].name);
    try std.testing.expectEqualStrings("other", settings[0].value);
}

test "usermgr database scoped app role settings override global settings durably" {
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

    var user = try manager.createUser("alice", "secret", &.{});
    defer user.deinit(alloc);
    try manager.createRoleSubject("role:app_writer");
    try manager.createRoleSubject("role:app_reader");
    try manager.addRoleToUser("alice", "role:app_writer");
    try manager.addRoleToUser("alice", "role:app_reader");

    try std.testing.expectError(error.InvalidRoleSetting, manager.setRoleDatabaseSetting("role:app_writer", "", "app.tenant_id", "acme"));
    try std.testing.expectError(error.InvalidRoleSetting, manager.setRoleDatabaseSetting("role:app_writer", "*", "app.tenant_id", "acme"));
    try std.testing.expectError(error.UnsupportedRoleSetting, manager.setRoleDatabaseSetting("role:app_writer", "appdb", "statement_timeout", "5s"));

    try manager.setRoleSetting("role:app_writer", "app.tenant_id", "global-role");
    try manager.setRoleDatabaseSetting("role:app_writer", "appdb", "app.tenant_id", "scoped-role");
    try manager.setRoleDatabaseSetting("role:app_writer", "appdb", "app.tenant_id", "scoped-role-replaced");
    const scoped = try manager.getRoleDatabaseSetting("role:app_writer", "appdb", "app.tenant_id");
    defer alloc.free(scoped);
    try std.testing.expectEqualStrings("scoped-role-replaced", scoped);

    const global_effective = try manager.getEffectiveRoleSettings("alice");
    defer {
        for (global_effective) |*setting| setting.deinit(alloc);
        alloc.free(global_effective);
    }
    try std.testing.expectEqual(@as(usize, 1), global_effective.len);
    try std.testing.expectEqualStrings("app.tenant_id", global_effective[0].name);
    try std.testing.expectEqualStrings("global-role", global_effective[0].value);

    const scoped_effective = try manager.getEffectiveRoleSettingsForDatabase("alice", "appdb");
    defer {
        for (scoped_effective) |*setting| setting.deinit(alloc);
        alloc.free(scoped_effective);
    }
    try std.testing.expectEqual(@as(usize, 1), scoped_effective.len);
    try std.testing.expectEqualStrings("app.tenant_id", scoped_effective[0].name);
    try std.testing.expectEqualStrings("scoped-role-replaced", scoped_effective[0].value);

    try manager.setRoleDatabaseSetting("role:app_reader", "appdb", "app.tenant_id", "conflict");
    try std.testing.expectError(error.RoleSettingConflict, manager.getEffectiveRoleSettingsForDatabase("alice", "appdb"));

    try manager.setRoleDatabaseSetting("alice", "appdb", "app.tenant_id", "direct-scoped");
    const direct_scoped_effective = try manager.getEffectiveRoleSettingsForDatabase("alice", "appdb");
    defer {
        for (direct_scoped_effective) |*setting| setting.deinit(alloc);
        alloc.free(direct_scoped_effective);
    }
    try std.testing.expectEqual(@as(usize, 1), direct_scoped_effective.len);
    try std.testing.expectEqualStrings("direct-scoped", direct_scoped_effective[0].value);

    try manager.removeRoleDatabaseSetting("alice", "appdb", "app.tenant_id");
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleDatabaseSetting("alice", "appdb", "app.tenant_id"));
}

test "usermgr runtime role settings are native role defaults with optional database scope" {
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

    try manager.createRoleSubject("role:app_writer");
    try std.testing.expectError(error.UnsupportedRoleSetting, manager.setRoleRuntimeSetting("role:app_writer", null, "unknown_guc", "on"));
    try std.testing.expectError(error.InvalidRoleSetting, manager.setRoleRuntimeSetting("role:app_writer", "", "statement_timeout", "5s"));
    try std.testing.expectError(error.InvalidRoleSetting, manager.setRoleRuntimeSetting("role:app_writer", "*", "statement_timeout", "5s"));
    try std.testing.expectError(error.InvalidRoleSetting, manager.setRoleRuntimeSetting("role:app_writer", null, "statement_timeout", "five seconds"));
    try std.testing.expectError(error.InvalidRoleSetting, manager.setRoleRuntimeSetting("role:app_writer", null, "statement_timeout", "5fortnights"));
    try std.testing.expectError(error.InvalidRoleSetting, manager.setRoleRuntimeSetting("role:app_writer", null, "search_path", "public,,analytics"));
    try std.testing.expectError(error.InvalidRoleSetting, manager.setRoleRuntimeSetting("role:app_writer", null, "timezone", "UTC;DROP"));

    try manager.setRoleRuntimeSetting("role:app_writer", null, "statement_timeout", "5s");
    try manager.setRoleRuntimeSetting("role:app_writer", null, "statement_timeout", "10s");
    const global_setting = try manager.getRoleRuntimeSetting("role:app_writer", null, "statement_timeout");
    defer alloc.free(global_setting);
    try std.testing.expectEqualStrings("10s", global_setting);
    try manager.setRoleRuntimeSetting("role:app_writer", null, "timezone", "America/Los_Angeles");
    try manager.setRoleRuntimeSetting("role:app_writer", null, "search_path", "analytics, public");

    try manager.setRoleRuntimeSetting("role:app_writer", "appdb", "statement_timeout", "1ms");
    const scoped_setting = try manager.getRoleRuntimeSetting("role:app_writer", "appdb", "statement_timeout");
    defer alloc.free(scoped_setting);
    try std.testing.expectEqualStrings("1ms", scoped_setting);

    try manager.removeRoleRuntimeSetting("role:app_writer", null, "statement_timeout");
    try std.testing.expectError(error.RoleSettingNotFound, manager.getRoleRuntimeSetting("role:app_writer", null, "statement_timeout"));
    const scoped_after_global_reset = try manager.getRoleRuntimeSetting("role:app_writer", "appdb", "statement_timeout");
    defer alloc.free(scoped_after_global_reset);
    try std.testing.expectEqualStrings("1ms", scoped_after_global_reset);
}

test "usermgr effective runtime role settings merge direct database overrides and inherited conflicts" {
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

    var user = try manager.createUser("alice", "secret", &.{});
    defer user.deinit(alloc);
    try manager.createRoleSubject("role:app_writer");
    try manager.createRoleSubject("role:app_reader");
    try manager.addRoleToUser("alice", "role:app_writer");
    try manager.addRoleToUser("alice", "role:app_reader");

    try manager.setRoleRuntimeSetting("role:app_writer", null, "statement_timeout", "5s");
    try manager.setRoleRuntimeSetting("role:app_reader", null, "statement_timeout", "5s");
    const inherited = try manager.getEffectiveRoleRuntimeSettings("alice");
    defer {
        for (inherited) |*setting| setting.deinit(alloc);
        alloc.free(inherited);
    }
    try std.testing.expectEqual(@as(usize, 1), inherited.len);
    try std.testing.expectEqualStrings("statement_timeout", inherited[0].name);
    try std.testing.expectEqualStrings("5s", inherited[0].value);

    try manager.setRoleRuntimeSetting("role:app_writer", "appdb", "statement_timeout", "1ms");
    try manager.setRoleRuntimeSetting("role:app_reader", "appdb", "statement_timeout", "1ms");
    const scoped = try manager.getEffectiveRoleRuntimeSettingsForDatabase("alice", "appdb");
    defer {
        for (scoped) |*setting| setting.deinit(alloc);
        alloc.free(scoped);
    }
    try std.testing.expectEqual(@as(usize, 1), scoped.len);
    try std.testing.expectEqualStrings("statement_timeout", scoped[0].name);
    try std.testing.expectEqualStrings("1ms", scoped[0].value);

    try manager.setRoleRuntimeSetting("alice", "appdb", "statement_timeout", "500us");
    const direct_scoped = try manager.getEffectiveRoleRuntimeSettingsForDatabase("alice", "appdb");
    defer {
        for (direct_scoped) |*setting| setting.deinit(alloc);
        alloc.free(direct_scoped);
    }
    try std.testing.expectEqual(@as(usize, 1), direct_scoped.len);
    try std.testing.expectEqualStrings("500us", direct_scoped[0].value);

    try manager.removeRoleRuntimeSetting("alice", "appdb", "statement_timeout");
    try manager.setRoleRuntimeSetting("role:app_reader", "appdb", "statement_timeout", "2ms");
    try std.testing.expectError(error.RoleSettingConflict, manager.getEffectiveRoleRuntimeSettingsForDatabase("alice", "appdb"));
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

test "usermgr SQL row security policy targets follow role membership durably" {
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

    var alice = try manager.createUser("alice", "secret", &.{});
    defer alice.deinit(alloc);
    var bob = try manager.createUser("bob", "secret", &.{});
    defer bob.deinit(alloc);
    try manager.createRoleSubject("role:tenant_reader");
    try manager.createRoleSubject("role:tenant_writer");
    try manager.addRoleToUser("alice", "role:tenant_reader");

    try manager.enableSqlRowSecurity("default.public.docs");
    try manager.createSqlRowSecurityPolicyWithTargets(
        "tenant_only",
        "default.public.docs",
        "{\"term\":{\"tenant_id\":\"acme\"}}",
        &.{"role:tenant_reader"},
    );
    try manager.createSqlRowSecurityPolicyWithTargets(
        "public_visibility",
        "default.public.docs",
        "{\"term\":{\"visibility\":\"public\"}}",
        &.{},
    );

    const alice_filters = try manager.getRowFilters("alice");
    defer {
        for (alice_filters) |*entry| entry.deinit(alloc);
        alloc.free(alice_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), alice_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, alice_filters[0].filter, "\"disjuncts\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, alice_filters[0].filter, "\"conjuncts\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, alice_filters[0].filter, "\"tenant_id\":\"acme\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, alice_filters[0].filter, "\"visibility\":\"public\"") != null);

    const bob_filters = try manager.getRowFilters("bob");
    defer {
        for (bob_filters) |*entry| entry.deinit(alloc);
        alloc.free(bob_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), bob_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, bob_filters[0].filter, "\"tenant_id\":\"acme\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, bob_filters[0].filter, "\"visibility\":\"public\"") != null);

    try std.testing.expectError(error.RoleInUse, manager.dropRoleSubject("role:tenant_reader"));
    try manager.dropRoleSubjectCascade("role:tenant_reader");
    const alice_after_cascade = try manager.getRowFilters("alice");
    defer {
        for (alice_after_cascade) |*entry| entry.deinit(alloc);
        alloc.free(alice_after_cascade);
    }
    try std.testing.expectEqual(@as(usize, 1), alice_after_cascade.len);
    try std.testing.expect(std.mem.indexOf(u8, alice_after_cascade[0].filter, "\"tenant_id\":\"acme\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, alice_after_cascade[0].filter, "\"visibility\":\"public\"") != null);
}

test "usermgr SQL row security policy target replacement updates effective filters" {
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

    var alice = try manager.createUser("alice", "secret", &.{});
    defer alice.deinit(alloc);
    var bob = try manager.createUser("bob", "secret", &.{});
    defer bob.deinit(alloc);

    try manager.createRoleSubject("role:app_reader");
    try manager.createRoleSubject("role:app_writer");
    try manager.enableSqlRowSecurity("docs");
    try manager.addRoleToUser("alice", "role:app_reader");
    try manager.addRoleToUser("bob", "role:app_writer");

    try manager.createSqlRowSecurityPolicyWithTargets(
        "docs_reader_policy",
        "docs",
        "{\"term\":{\"tenant_id\":\"reader\"}}",
        &.{"role:app_reader"},
    );
    try manager.createSqlRowSecurityPolicyWithTargets(
        "docs_public_policy",
        "docs",
        "{\"term\":{\"visibility\":\"public\"}}",
        &.{},
    );

    try manager.replaceSqlRowSecurityPolicyWithTargets(
        "docs_reader_policy",
        "docs",
        "{\"term\":{\"tenant_id\":\"reader\"}}",
        &.{"role:app_writer"},
    );

    const targets = try manager.getSqlRowSecurityPolicyTargets("docs_reader_policy", "docs");
    defer {
        for (targets) |target| alloc.free(@constCast(target));
        alloc.free(targets);
    }
    try std.testing.expectEqual(@as(usize, 1), targets.len);
    try std.testing.expectEqualStrings("role:app_writer", targets[0]);

    const alice_filters = try manager.getRowFilters("alice");
    defer {
        for (alice_filters) |*entry| entry.deinit(alloc);
        alloc.free(alice_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), alice_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, alice_filters[0].filter, "\"visibility\":\"public\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, alice_filters[0].filter, "\"tenant_id\":\"reader\"") == null);

    const bob_filters = try manager.getRowFilters("bob");
    defer {
        for (bob_filters) |*entry| entry.deinit(alloc);
        alloc.free(bob_filters);
    }
    try std.testing.expectEqual(@as(usize, 1), bob_filters.len);
    try std.testing.expect(std.mem.indexOf(u8, bob_filters[0].filter, "\"visibility\":\"public\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, bob_filters[0].filter, "\"tenant_id\":\"reader\"") != null);
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
