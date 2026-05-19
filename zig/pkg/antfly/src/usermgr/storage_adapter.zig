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
const storage = @import("usermgr_storage");
const user_manager = @import("user_manager.zig");

const Allocator = std.mem.Allocator;
const backend_erased = storage.backend_erased;
const backend_types = backend_erased.types;
const lsm_backend = storage.lsm_backend;

pub const users_namespace: backend_types.Namespace = .{ .name = "usermgr_users" };
pub const casbin_namespace: backend_types.Namespace = .{ .name = "usermgr_casbin" };

const PersistedPermission = struct {
    resource: []const u8,
    resource_type: []const u8,
    type: []const u8,
};

const PersistedRowFilterEntry = struct {
    table: []const u8,
    filter: []const u8,
};

const PersistedApiKey = struct {
    key_id: []const u8,
    secret_hash: []const u8,
    secret_salt: []const u8,
    username: []const u8,
    name: []const u8,
    permissions: []const PersistedPermission,
    row_filter: []const PersistedRowFilterEntry,
    created_at_ns: u64,
    expires_at_ns: ?u64 = null,
};

pub const StorageUserStore = struct {
    alloc: Allocator,
    store: backend_erased.NamespaceStore,

    const iface_vtable: user_manager.UserStore.VTable = .{
        .load_users = loadUsers,
        .save_user = saveUser,
        .delete_user = deleteUser,
        .load_api_keys = loadApiKeys,
        .save_api_key = saveApiKey,
        .delete_api_key = deleteApiKey,
    };

    pub fn init(alloc: Allocator, store: backend_erased.NamespaceStore) StorageUserStore {
        return .{ .alloc = alloc, .store = store };
    }

    pub fn iface(self: *StorageUserStore) user_manager.UserStore {
        return .{
            .ptr = self,
            .vtable = &iface_vtable,
        };
    }

    fn loadUsers(ptr: *anyopaque, alloc: Allocator) ![]user_manager.User {
        const self: *StorageUserStore = @ptrCast(@alignCast(ptr));
        var txn = try self.store.beginRead();
        defer txn.abort();

        var cur = try txn.openCursor(users_namespace);
        defer cur.close();

        var out = std.ArrayList(user_manager.User).empty;
        errdefer {
            for (out.items) |*user| user.deinit(alloc);
            out.deinit(alloc);
        }

        var entry_opt = try cur.first();
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            if (!std.mem.startsWith(u8, entry.key, "userpass:")) continue;
            const username = entry.key["userpass:".len..];
            const metadata_key = try std.fmt.allocPrint(alloc, "usermeta:{s}", .{username});
            defer alloc.free(metadata_key);
            const metadata_json = txn.get(users_namespace, metadata_key) catch |err| switch (err) {
                error.NotFound => "{}",
                else => return err,
            };
            try out.append(alloc, .{
                .username = try alloc.dupe(u8, username),
                .password_hash = try alloc.dupe(u8, entry.value),
                .metadata_json = try alloc.dupe(u8, metadata_json),
            });
        }
        return try out.toOwnedSlice(alloc);
    }

    fn saveUser(ptr: *anyopaque, alloc: Allocator, user: *const user_manager.User) !void {
        const self: *StorageUserStore = @ptrCast(@alignCast(ptr));
        _ = alloc;
        var txn = try self.store.beginWrite();
        errdefer txn.abort();
        const key = try std.fmt.allocPrint(self.alloc, "userpass:{s}", .{user.username});
        defer self.alloc.free(key);
        const metadata_key = try std.fmt.allocPrint(self.alloc, "usermeta:{s}", .{user.username});
        defer self.alloc.free(metadata_key);
        try txn.put(users_namespace, key, user.password_hash);
        try txn.put(users_namespace, metadata_key, if (user.metadata_json.len > 0) user.metadata_json else "{}");
        try txn.commit();
    }

    fn deleteUser(ptr: *anyopaque, username: []const u8) !bool {
        const self: *StorageUserStore = @ptrCast(@alignCast(ptr));
        var txn = try self.store.beginWrite();
        errdefer txn.abort();
        const key = try std.fmt.allocPrint(self.alloc, "userpass:{s}", .{username});
        defer self.alloc.free(key);
        const existed = txn.get(users_namespace, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existed != null) {
            try txn.delete(users_namespace, key);
            const metadata_key = try std.fmt.allocPrint(self.alloc, "usermeta:{s}", .{username});
            defer self.alloc.free(metadata_key);
            if ((txn.get(users_namespace, metadata_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            }) != null) {
                try txn.delete(users_namespace, metadata_key);
            }
            try txn.commit();
            return true;
        }
        txn.abort();
        return false;
    }

    fn loadApiKeys(ptr: *anyopaque, alloc: Allocator) ![]user_manager.ApiKeyRecord {
        const self: *StorageUserStore = @ptrCast(@alignCast(ptr));
        var txn = try self.store.beginRead();
        defer txn.abort();

        var cur = try txn.openCursor(users_namespace);
        defer cur.close();

        var out = std.ArrayList(user_manager.ApiKeyRecord).empty;
        errdefer {
            for (out.items) |*record| record.deinit(alloc);
            out.deinit(alloc);
        }

        var entry_opt = try cur.first();
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            if (!std.mem.startsWith(u8, entry.key, "apikey:")) continue;
            var parsed = try std.json.parseFromSlice(PersistedApiKey, alloc, entry.value, .{
                .allocate = .alloc_always,
            });
            defer parsed.deinit();
            try out.append(alloc, try recordFromPersisted(alloc, parsed.value));
        }
        return try out.toOwnedSlice(alloc);
    }

    fn saveApiKey(ptr: *anyopaque, alloc: Allocator, record: *const user_manager.ApiKeyRecord) !void {
        const self: *StorageUserStore = @ptrCast(@alignCast(ptr));
        _ = alloc;
        var txn = try self.store.beginWrite();
        errdefer txn.abort();
        const key = try std.fmt.allocPrint(self.alloc, "apikey:{s}", .{record.key.key_id});
        defer self.alloc.free(key);
        const body = try encodePersistedApiKey(self.alloc, record.*);
        defer self.alloc.free(body);
        try txn.put(users_namespace, key, body);
        try txn.commit();
    }

    fn deleteApiKey(ptr: *anyopaque, key_id: []const u8) !bool {
        const self: *StorageUserStore = @ptrCast(@alignCast(ptr));
        var txn = try self.store.beginWrite();
        errdefer txn.abort();
        const key = try std.fmt.allocPrint(self.alloc, "apikey:{s}", .{key_id});
        defer self.alloc.free(key);
        const existed = txn.get(users_namespace, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existed != null) {
            try txn.delete(users_namespace, key);
            try txn.commit();
            return true;
        }
        txn.abort();
        return false;
    }
};

pub const StorageCasbinAdapter = struct {
    alloc: Allocator,
    store: backend_erased.NamespaceStore,

    const iface_vtable: casbin.Adapter.VTable = .{
        .load_policies = loadPolicies,
        .add_policies = addPolicies,
        .remove_policies = removePolicies,
        .remove_filtered_policy = removeFilteredPolicy,
    };

    pub fn init(alloc: Allocator, store: backend_erased.NamespaceStore) StorageCasbinAdapter {
        return .{ .alloc = alloc, .store = store };
    }

    pub fn iface(self: *StorageCasbinAdapter) casbin.Adapter {
        return .{
            .ptr = self,
            .vtable = &iface_vtable,
        };
    }

    fn loadPolicies(ptr: *anyopaque, alloc: Allocator) ![]casbin.Rule {
        const self: *StorageCasbinAdapter = @ptrCast(@alignCast(ptr));
        var txn = try self.store.beginRead();
        defer txn.abort();

        var cur = try txn.openCursor(casbin_namespace);
        defer cur.close();

        var out = std.ArrayList(casbin.Rule).empty;
        errdefer {
            for (out.items) |*rule| rule.deinit(alloc);
            out.deinit(alloc);
        }

        var entry_opt = try cur.first();
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            try out.append(alloc, try ruleFromKey(alloc, entry.key));
        }
        return try out.toOwnedSlice(alloc);
    }

    fn addPolicies(ptr: *anyopaque, alloc: Allocator, rules: []const casbin.Rule) !void {
        const self: *StorageCasbinAdapter = @ptrCast(@alignCast(ptr));
        _ = alloc;
        var txn = try self.store.beginBatch();
        errdefer txn.abort();
        for (rules) |rule| {
            const key = try keyFromRule(self.alloc, rule);
            defer self.alloc.free(key);
            try txn.put(casbin_namespace, key, "1");
        }
        try txn.commit();
    }

    fn removePolicies(ptr: *anyopaque, rules: []const casbin.Rule) !void {
        const self: *StorageCasbinAdapter = @ptrCast(@alignCast(ptr));
        var txn = try self.store.beginBatch();
        errdefer txn.abort();
        for (rules) |rule| {
            const key = try keyFromRule(self.alloc, rule);
            defer self.alloc.free(key);
            try txn.delete(casbin_namespace, key);
        }
        try txn.commit();
    }

    fn removeFilteredPolicy(
        ptr: *anyopaque,
        ptype: []const u8,
        field_index: usize,
        field_values: []const []const u8,
    ) !usize {
        const self: *StorageCasbinAdapter = @ptrCast(@alignCast(ptr));
        var read_txn = try self.store.beginRead();
        defer read_txn.abort();

        var cur = try read_txn.openCursor(casbin_namespace);
        defer cur.close();

        var keys = std.ArrayList([]u8).empty;
        defer {
            for (keys.items) |key| self.alloc.free(key);
            keys.deinit(self.alloc);
        }

        var entry_opt = try cur.first();
        while (entry_opt) |entry| : (entry_opt = try cur.next()) {
            var rule = try ruleFromKey(self.alloc, entry.key);
            defer rule.deinit(self.alloc);
            if (!ruleMatchesFilter(rule, ptype, field_index, field_values)) continue;
            try keys.append(self.alloc, try self.alloc.dupe(u8, entry.key));
        }

        if (keys.items.len == 0) return 0;

        var write_txn = try self.store.beginBatch();
        errdefer write_txn.abort();
        for (keys.items) |key| try write_txn.delete(casbin_namespace, key);
        try write_txn.commit();
        return keys.items.len;
    }
};

fn keyFromRule(alloc: Allocator, rule: casbin.Rule) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, rule.ptype);
    for (rule.fields) |field| {
        try out.appendSlice(alloc, "::");
        try out.appendSlice(alloc, field);
    }
    return try out.toOwnedSlice(alloc);
}

fn ruleFromKey(alloc: Allocator, key: []const u8) !casbin.Rule {
    var parts = std.ArrayList([]const u8).empty;
    defer parts.deinit(alloc);
    var iter = std.mem.splitSequence(u8, key, "::");
    while (iter.next()) |part| {
        try parts.append(alloc, part);
    }
    if (parts.items.len == 0) return error.InvalidCasbinKey;
    return try casbin.Rule.initOwned(alloc, parts.items[0], parts.items[1..]);
}

fn ruleMatchesFilter(
    rule: casbin.Rule,
    ptype: []const u8,
    field_index: usize,
    field_values: []const []const u8,
) bool {
    if (!std.mem.eql(u8, rule.ptype, ptype)) return false;
    for (field_values, 0..) |value, i| {
        if (value.len == 0) continue;
        const idx = field_index + i;
        if (idx >= rule.fields.len) return false;
        if (!std.mem.eql(u8, rule.fields[idx], value)) return false;
    }
    return true;
}

fn encodePersistedApiKey(alloc: Allocator, record: user_manager.ApiKeyRecord) ![]u8 {
    const permissions = try alloc.alloc(PersistedPermission, record.key.permissions.len);
    defer alloc.free(permissions);
    for (record.key.permissions, 0..) |perm, i| {
        permissions[i] = .{
            .resource = perm.resource,
            .resource_type = perm.resource_type.slice(),
            .type = perm.type.slice(),
        };
    }

    const row_filter = try alloc.alloc(PersistedRowFilterEntry, record.key.row_filter.len);
    defer alloc.free(row_filter);
    for (record.key.row_filter, 0..) |entry, i| {
        row_filter[i] = .{
            .table = entry.table,
            .filter = entry.filter,
        };
    }

    const secret_hash = try encodeBase64(alloc, record.secret_hash);
    defer alloc.free(secret_hash);
    const secret_salt = try encodeBase64(alloc, record.secret_salt);
    defer alloc.free(secret_salt);

    return try std.fmt.allocPrint(alloc, "{f}", .{
        std.json.fmt(PersistedApiKey{
            .key_id = record.key.key_id,
            .secret_hash = secret_hash,
            .secret_salt = secret_salt,
            .username = record.key.username,
            .name = record.key.name,
            .permissions = permissions,
            .row_filter = row_filter,
            .created_at_ns = record.key.created_at_ns,
            .expires_at_ns = record.key.expires_at_ns,
        }, .{}),
    });
}

fn recordFromPersisted(alloc: Allocator, persisted: PersistedApiKey) !user_manager.ApiKeyRecord {
    const permissions = try alloc.alloc(user_manager.Permission, persisted.permissions.len);
    errdefer alloc.free(permissions);
    var filled_perms: usize = 0;
    errdefer for (permissions[0..filled_perms]) |*perm| perm.deinit(alloc);
    for (persisted.permissions, 0..) |perm, i| {
        permissions[i] = try user_manager.Permission.initOwned(
            alloc,
            try user_manager.ResourceType.fromSlice(perm.resource_type),
            perm.resource,
            try user_manager.PermissionType.fromSlice(perm.type),
        );
        filled_perms += 1;
    }

    const row_filter = try alloc.alloc(user_manager.RowFilterEntry, persisted.row_filter.len);
    errdefer alloc.free(row_filter);
    var filled_filters: usize = 0;
    errdefer for (row_filter[0..filled_filters]) |*entry| entry.deinit(alloc);
    for (persisted.row_filter, 0..) |entry, i| {
        row_filter[i] = try user_manager.RowFilterEntry.initOwned(alloc, entry.table, entry.filter);
        filled_filters += 1;
    }

    return .{
        .key = .{
            .key_id = try alloc.dupe(u8, persisted.key_id),
            .username = try alloc.dupe(u8, persisted.username),
            .name = try alloc.dupe(u8, persisted.name),
            .permissions = permissions,
            .row_filter = row_filter,
            .created_at_ns = persisted.created_at_ns,
            .expires_at_ns = persisted.expires_at_ns,
        },
        .secret_hash = try decodeBase64Owned(alloc, persisted.secret_hash),
        .secret_salt = try decodeBase64Owned(alloc, persisted.secret_salt),
    };
}

fn encodeBase64(alloc: Allocator, raw: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(raw.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, raw);
    return out;
}

fn decodeBase64Owned(alloc: Allocator, raw: []const u8) ![]u8 {
    const size = try std.base64.standard.Decoder.calcSizeForSlice(raw);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, raw);
    return out;
}

test "storage-backed user store and casbin adapter persist usermgr state" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 1 });
    defer backend.close();

    var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
    defer runtime.deinit();

    var user_store = StorageUserStore.init(std.testing.allocator, runtime);
    var casbin_store = StorageCasbinAdapter.init(std.testing.allocator, runtime);
    var manager = try user_manager.UserManager.init(
        std.testing.allocator,
        user_store.iface(),
        try casbin.Enforcer.init(
            std.testing.allocator,
            try casbin.Model.fromString(std.testing.allocator,
                \\[request_definition]
                \\r = sub, typ, obj, act
                \\[policy_definition]
                \\p = sub, typ, obj, act
                \\p2 = sub, obj, filter
                \\[role_definition]
                \\g = _, _
                \\[matchers]
                \\m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
            ),
            casbin_store.iface(),
        ),
    );
    defer manager.deinit();

    var initial = [_]user_manager.Permission{
        try user_manager.Permission.initOwned(std.testing.allocator, .table, "docs", .read),
    };
    defer initial[0].deinit(std.testing.allocator);

    var created = try manager.createUserWithMetadata("alice", "secret", &initial, "{\"tenant_id\":\"acme\"}");
    defer created.deinit(std.testing.allocator);
    try manager.setRowFilter("alice", "docs", "{\"term\":{\"team\":\"eng\"}}");
    try std.testing.expect(try manager.enforce("alice", .table, "docs", .read));

    var reloaded_user_store = StorageUserStore.init(std.testing.allocator, runtime);
    var reloaded_casbin_store = StorageCasbinAdapter.init(std.testing.allocator, runtime);
    var reloaded = try user_manager.UserManager.init(
        std.testing.allocator,
        reloaded_user_store.iface(),
        try casbin.Enforcer.init(
            std.testing.allocator,
            try casbin.Model.fromString(std.testing.allocator,
                \\[request_definition]
                \\r = sub, typ, obj, act
                \\[policy_definition]
                \\p = sub, typ, obj, act
                \\p2 = sub, obj, filter
                \\[role_definition]
                \\g = _, _
                \\[matchers]
                \\m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
            ),
            reloaded_casbin_store.iface(),
        ),
    );
    defer reloaded.deinit();

    var authed = try reloaded.authenticateUser("alice", "secret");
    defer authed.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"acme\"}", authed.metadata_json);
    try std.testing.expect(try reloaded.enforce("alice", .table, "docs", .read));
    const filter = try reloaded.getRowFilter("alice", "docs");
    defer std.testing.allocator.free(filter);
    try std.testing.expect(std.mem.indexOf(u8, filter, "\"team\":\"eng\"") != null);

    var api_row_filter = [_]user_manager.RowFilterEntry{
        try user_manager.RowFilterEntry.initOwned(std.testing.allocator, "docs", "{\"term\":{\"tier\":\"gold\"}}"),
    };
    defer api_row_filter[0].deinit(std.testing.allocator);
    var api_key = try reloaded.createApiKey("alice", "ci", &initial, &api_row_filter, null);
    defer api_key.deinit(std.testing.allocator);

    var validated = try reloaded.validateApiKey(api_key.key.key_id, api_key.key_secret);
    defer validated.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("alice", validated.username);
    try std.testing.expectEqualStrings("{\"tenant_id\":\"acme\"}", validated.metadata_json);
    try std.testing.expectEqual(@as(usize, 1), validated.permissions.len);
    try std.testing.expectEqual(@as(usize, 1), validated.row_filter.len);
}
