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
const object_storage = @import("../../storage/object_storage.zig");
const catalog_types = @import("types.zig");
const catalog_store = @import("store.zig");
const object_store_support = @import("../object_store_support.zig");

const PersistedNamespace = struct {
    name: []const u8,
    created_at_ns: u64,
    policy: catalog_types.NamespacePolicy = .{},
};

const PersistedTableBinding = struct {
    table_name: []const u8,
    namespace: []const u8,
    schema_json: []const u8 = "",
    read_schema_json: []const u8 = "",
    indexes_json: []const u8 = "{}",
};

const TableBinding = struct {
    table_name: []u8,
    namespace: []u8,
    schema_json: []u8,
    read_schema_json: []u8,
    indexes_json: []u8,

    fn deinit(self: *TableBinding, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.namespace);
        alloc.free(self.schema_json);
        alloc.free(self.read_schema_json);
        alloc.free(self.indexes_json);
        self.* = undefined;
    }
};

pub const ObjectStore = struct {
    alloc: std.mem.Allocator,
    opened: object_store_support.OpenedObjectStore,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn initRemoteUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initRemoteUri(alloc, uri, "serverless-catalog"),
        };
    }

    pub fn initFileUri(alloc: std.mem.Allocator, uri: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initFileUri(alloc, uri, "serverless-catalog"),
        };
    }

    pub fn initGcsUri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initGcsUri(alloc, bucket, prefix),
        };
    }

    pub fn initS3Uri(alloc: std.mem.Allocator, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initS3Uri(alloc, bucket, prefix),
        };
    }

    pub fn initWithClient(alloc: std.mem.Allocator, client: object_storage.ObjectStorage, bucket: []const u8, prefix: []const u8) !ObjectStore {
        return .{
            .alloc = alloc,
            .opened = try object_store_support.OpenedObjectStore.initWithClient(alloc, client, bucket, prefix),
        };
    }

    pub fn deinit(self: *ObjectStore) void {
        self.opened.deinit();
        self.* = undefined;
    }

    pub fn catalogStore(self: *ObjectStore) catalog_store.CatalogStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn ensureNamespace(self: *ObjectStore, name: []const u8, created_at_ns: u64, policy: catalog_types.NamespacePolicy) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var current = try self.tryReadState(self.alloc);
        defer if (current) |*value| {
            freeNamespaceRecords(self.alloc, value.records);
            if (value.etag) |etag| self.alloc.free(etag);
        };

        for (if (current) |value| value.records else &.{}) |record| {
            if (std.mem.eql(u8, record.name, name)) return false;
        }

        const existing = if (current) |value| value.records else &.{};
        const updated = try self.alloc.alloc(catalog_types.NamespaceRecord, existing.len + 1);
        errdefer self.alloc.free(updated);
        var initialized: usize = 0;
        errdefer freeNamespaceRecords(self.alloc, updated[0..initialized]);

        for (existing, 0..) |record, idx| {
            updated[idx] = .{
                .name = try self.alloc.dupe(u8, record.name),
                .created_at_ns = record.created_at_ns,
                .policy = record.policy,
            };
            initialized += 1;
        }
        updated[existing.len] = .{
            .name = try self.alloc.dupe(u8, name),
            .created_at_ns = created_at_ns,
            .policy = policy,
        };
        initialized += 1;

        try self.writeState(updated, if (current) |value| value.etag else null);
        freeNamespaceRecords(self.alloc, updated);
        return true;
    }

    pub fn ensureTable(
        self: *ObjectStore,
        table_name: []const u8,
        namespace: []const u8,
        created_at_ns: u64,
        policy: catalog_types.NamespacePolicy,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var current_bindings = try self.tryReadTableBindings(self.alloc);
        defer if (current_bindings) |*value| {
            freeTableBindings(self.alloc, value.records);
            if (value.etag) |etag| self.alloc.free(etag);
        };

        for (if (current_bindings) |value| value.records else &.{}) |binding| {
            if (std.mem.eql(u8, binding.table_name, table_name)) return false;
            if (std.mem.eql(u8, binding.namespace, namespace)) return error.NamespaceAlreadyMapped;
        }

        var current_namespaces = try self.tryReadState(self.alloc);
        defer if (current_namespaces) |*value| {
            freeNamespaceRecords(self.alloc, value.records);
            if (value.etag) |etag| self.alloc.free(etag);
        };

        const namespaces = if (current_namespaces) |value| value.records else &.{};
        var namespace_exists = false;
        for (namespaces) |record| {
            if (std.mem.eql(u8, record.name, namespace)) {
                namespace_exists = true;
                break;
            }
        }

        if (!namespace_exists) {
            const updated_namespaces = try self.alloc.alloc(catalog_types.NamespaceRecord, namespaces.len + 1);
            errdefer self.alloc.free(updated_namespaces);
            var initialized_namespaces: usize = 0;
            errdefer freeNamespaceRecords(self.alloc, updated_namespaces[0..initialized_namespaces]);

            for (namespaces, 0..) |record, idx| {
                updated_namespaces[idx] = .{
                    .name = try self.alloc.dupe(u8, record.name),
                    .created_at_ns = record.created_at_ns,
                    .policy = record.policy,
                };
                initialized_namespaces += 1;
            }
            updated_namespaces[namespaces.len] = .{
                .name = try self.alloc.dupe(u8, namespace),
                .created_at_ns = created_at_ns,
                .policy = policy,
            };
            initialized_namespaces += 1;

            try self.writeState(updated_namespaces, if (current_namespaces) |value| value.etag else null);
            freeNamespaceRecords(self.alloc, updated_namespaces);
        }

        const bindings = if (current_bindings) |value| value.records else &.{};
        const updated_bindings = try self.alloc.alloc(TableBinding, bindings.len + 1);
        errdefer self.alloc.free(updated_bindings);
        var initialized_bindings: usize = 0;
        errdefer freeTableBindings(self.alloc, updated_bindings[0..initialized_bindings]);

        for (bindings, 0..) |binding, idx| {
            updated_bindings[idx] = .{
                .table_name = try self.alloc.dupe(u8, binding.table_name),
                .namespace = try self.alloc.dupe(u8, binding.namespace),
                .schema_json = try self.alloc.dupe(u8, binding.schema_json),
                .read_schema_json = try self.alloc.dupe(u8, binding.read_schema_json),
                .indexes_json = try self.alloc.dupe(u8, binding.indexes_json),
            };
            initialized_bindings += 1;
        }
        updated_bindings[bindings.len] = .{
            .table_name = try self.alloc.dupe(u8, table_name),
            .namespace = try self.alloc.dupe(u8, namespace),
            .schema_json = try self.alloc.dupe(u8, schema_json),
            .read_schema_json = try self.alloc.dupe(u8, read_schema_json),
            .indexes_json = try self.alloc.dupe(u8, indexes_json),
        };
        initialized_bindings += 1;

        try self.writeTableBindings(updated_bindings, if (current_bindings) |value| value.etag else null);
        freeTableBindings(self.alloc, updated_bindings);
        return true;
    }

    pub fn listNamespacesAlloc(self: *ObjectStore, alloc: std.mem.Allocator) ![]catalog_types.NamespaceRecord {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = try self.tryReadState(alloc);
        defer if (current) |*value| {
            freeNamespaceRecords(alloc, value.records);
            if (value.etag) |etag| alloc.free(etag);
        };

        const records = if (current) |value| try cloneNamespaceRecordsAlloc(alloc, value.records) else try alloc.alloc(catalog_types.NamespaceRecord, 0);
        std.mem.sort(catalog_types.NamespaceRecord, records, {}, lessNamespaceRecord);
        return records;
    }

    pub fn listTablesAlloc(self: *ObjectStore, alloc: std.mem.Allocator) ![]catalog_types.TableNamespaceRecord {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current_namespaces = try self.tryReadState(alloc);
        defer if (current_namespaces) |*value| {
            freeNamespaceRecords(alloc, value.records);
            if (value.etag) |etag| alloc.free(etag);
        };
        const current_bindings = try self.tryReadTableBindings(alloc);
        defer if (current_bindings) |*value| {
            freeTableBindings(alloc, value.records);
            if (value.etag) |etag| alloc.free(etag);
        };

        const namespaces = if (current_namespaces) |value| value.records else &.{};
        const bindings = if (current_bindings) |value| value.records else &.{};

        const out = try alloc.alloc(catalog_types.TableNamespaceRecord, bindings.len);
        errdefer alloc.free(out);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*record| record.deinit(alloc);
        }

        for (bindings, 0..) |binding, idx| {
            const namespace_record = findNamespaceRecord(namespaces, binding.namespace) orelse return error.NamespaceNotFound;
            out[idx] = .{
                .table_name = try alloc.dupe(u8, binding.table_name),
                .namespace = try alloc.dupe(u8, binding.namespace),
                .created_at_ns = namespace_record.created_at_ns,
                .policy = namespace_record.policy,
                .schema_json = try alloc.dupe(u8, binding.schema_json),
                .read_schema_json = try alloc.dupe(u8, binding.read_schema_json),
                .indexes_json = try alloc.dupe(u8, binding.indexes_json),
            };
            initialized += 1;
        }
        std.mem.sort(catalog_types.TableNamespaceRecord, out, {}, lessTableRecord);
        return out;
    }

    pub fn getTableAlloc(self: *ObjectStore, alloc: std.mem.Allocator, table_name: []const u8) !?catalog_types.TableNamespaceRecord {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current_namespaces = try self.tryReadState(alloc);
        defer if (current_namespaces) |*value| {
            freeNamespaceRecords(alloc, value.records);
            if (value.etag) |etag| alloc.free(etag);
        };
        const current_bindings = try self.tryReadTableBindings(alloc);
        defer if (current_bindings) |*value| {
            freeTableBindings(alloc, value.records);
            if (value.etag) |etag| alloc.free(etag);
        };

        const namespaces = if (current_namespaces) |value| value.records else &.{};
        const bindings = if (current_bindings) |value| value.records else &.{};
        for (bindings) |binding| {
            if (!std.mem.eql(u8, binding.table_name, table_name)) continue;
            const namespace_record = findNamespaceRecord(namespaces, binding.namespace) orelse return error.NamespaceNotFound;
            return .{
                .table_name = try alloc.dupe(u8, binding.table_name),
                .namespace = try alloc.dupe(u8, binding.namespace),
                .created_at_ns = namespace_record.created_at_ns,
                .policy = namespace_record.policy,
                .schema_json = try alloc.dupe(u8, binding.schema_json),
                .read_schema_json = try alloc.dupe(u8, binding.read_schema_json),
                .indexes_json = try alloc.dupe(u8, binding.indexes_json),
            };
        }
        return null;
    }

    pub fn setTableDefinition(
        self: *ObjectStore,
        table_name: []const u8,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var current_bindings = try self.tryReadTableBindings(self.alloc);
        defer if (current_bindings) |*value| {
            freeTableBindings(self.alloc, value.records);
            if (value.etag) |etag| self.alloc.free(etag);
        };

        const bindings = if (current_bindings) |value| value.records else &.{};
        const updated_bindings = try self.alloc.alloc(TableBinding, bindings.len);
        errdefer self.alloc.free(updated_bindings);
        var initialized_bindings: usize = 0;
        errdefer freeTableBindings(self.alloc, updated_bindings[0..initialized_bindings]);

        var found = false;
        for (bindings, 0..) |binding, idx| {
            const is_target = std.mem.eql(u8, binding.table_name, table_name);
            updated_bindings[idx] = .{
                .table_name = try self.alloc.dupe(u8, binding.table_name),
                .namespace = try self.alloc.dupe(u8, binding.namespace),
                .schema_json = try self.alloc.dupe(u8, if (is_target) schema_json else binding.schema_json),
                .read_schema_json = try self.alloc.dupe(u8, if (is_target) read_schema_json else binding.read_schema_json),
                .indexes_json = try self.alloc.dupe(u8, if (is_target) indexes_json else binding.indexes_json),
            };
            initialized_bindings += 1;
            if (is_target) found = true;
        }
        if (!found) return false;

        try self.writeTableBindings(updated_bindings, if (current_bindings) |value| value.etag else null);
        freeTableBindings(self.alloc, updated_bindings);
        return true;
    }

    pub fn resolveNamespaceAlloc(self: *ObjectStore, alloc: std.mem.Allocator, table_name: []const u8) ![]u8 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = try self.tryReadTableBindings(alloc);
        defer if (current) |*value| {
            freeTableBindings(alloc, value.records);
            if (value.etag) |etag| alloc.free(etag);
        };

        for (if (current) |value| value.records else &.{}) |binding| {
            if (std.mem.eql(u8, binding.table_name, table_name)) {
                return try alloc.dupe(u8, binding.namespace);
            }
        }
        return error.TableNotFound;
    }

    pub fn getPolicy(self: *ObjectStore, namespace: []const u8) !catalog_types.NamespacePolicy {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = try self.tryReadState(self.alloc);
        defer if (current) |*value| {
            freeNamespaceRecords(self.alloc, value.records);
            if (value.etag) |etag| self.alloc.free(etag);
        };

        const records = if (current) |value| value.records else &.{};
        for (records) |record| {
            if (std.mem.eql(u8, record.name, namespace)) return record.policy;
        }
        return error.NamespaceNotFound;
    }

    pub fn setPolicy(self: *ObjectStore, namespace: []const u8, policy: catalog_types.NamespacePolicy) !catalog_types.NamespacePolicy {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var current = try self.tryReadState(self.alloc);
        defer if (current) |*value| {
            freeNamespaceRecords(self.alloc, value.records);
            if (value.etag) |etag| self.alloc.free(etag);
        };

        const records = if (current) |value| value.records else return error.NamespaceNotFound;
        var found = false;
        for (records) |*record| {
            if (!std.mem.eql(u8, record.name, namespace)) continue;
            record.policy = policy;
            found = true;
            break;
        }
        if (!found) return error.NamespaceNotFound;

        try self.writeState(records, current.?.etag);
        return policy;
    }

    const CurrentState = struct {
        records: []catalog_types.NamespaceRecord,
        etag: ?[]u8,
    };

    const CurrentTableBindings = struct {
        records: []TableBinding,
        etag: ?[]u8,
    };

    fn tryReadState(self: *ObjectStore, alloc: std.mem.Allocator) !?CurrentState {
        const key = try stateKeyAlloc(alloc, self.opened.prefix);
        defer alloc.free(key);

        var result = self.opened.client.getObject(self.opened.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(alloc);

        const records = try decodeStateAlloc(alloc, result.body);
        return .{
            .records = records,
            .etag = if (result.metadata.etag) |value| try alloc.dupe(u8, value) else null,
        };
    }

    fn tryReadTableBindings(self: *ObjectStore, alloc: std.mem.Allocator) !?CurrentTableBindings {
        const key = try tableBindingsKeyAlloc(alloc, self.opened.prefix);
        defer alloc.free(key);

        var result = self.opened.client.getObject(self.opened.bucket, key, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer result.deinit(alloc);

        const records = try decodeTableBindingsAlloc(alloc, result.body);
        return .{
            .records = records,
            .etag = if (result.metadata.etag) |value| try alloc.dupe(u8, value) else null,
        };
    }

    fn writeState(self: *ObjectStore, records: []const catalog_types.NamespaceRecord, expected_etag: ?[]const u8) !void {
        const key = try stateKeyAlloc(self.alloc, self.opened.prefix);
        defer self.alloc.free(key);
        const encoded = try encodeStateAlloc(self.alloc, records);
        defer self.alloc.free(encoded);

        var result = try self.opened.client.putObject(self.opened.bucket, key, encoded, .{
            .content_type = "application/json",
            .if_none_match = expected_etag == null,
            .if_match_etag = expected_etag,
        });
        defer result.deinit(self.alloc);
    }

    fn writeTableBindings(self: *ObjectStore, bindings: []const TableBinding, expected_etag: ?[]const u8) !void {
        const key = try tableBindingsKeyAlloc(self.alloc, self.opened.prefix);
        defer self.alloc.free(key);
        const encoded = try encodeTableBindingsAlloc(self.alloc, bindings);
        defer self.alloc.free(encoded);

        var result = try self.opened.client.putObject(self.opened.bucket, key, encoded, .{
            .content_type = "application/json",
            .if_none_match = expected_etag == null,
            .if_match_etag = expected_etag,
        });
        defer result.deinit(self.alloc);
    }

    const vtable: catalog_store.CatalogStore.VTable = .{
        .deinit = erasedDeinit,
        .ensure_namespace = erasedEnsureNamespace,
        .ensure_table = erasedEnsureTable,
        .list_namespaces_alloc = erasedListNamespacesAlloc,
        .list_tables_alloc = erasedListTablesAlloc,
        .get_table_alloc = erasedGetTableAlloc,
        .set_table_definition = erasedSetTableDefinition,
        .resolve_namespace_alloc = erasedResolveNamespaceAlloc,
        .get_policy = erasedGetPolicy,
        .set_policy = erasedSetPolicy,
    };

    fn erasedDeinit(_: std.mem.Allocator, ptr: *anyopaque) void {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedEnsureNamespace(ptr: *anyopaque, name: []const u8, created_at_ns: u64, policy: catalog_types.NamespacePolicy) !bool {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.ensureNamespace(name, created_at_ns, policy);
    }

    fn erasedEnsureTable(
        ptr: *anyopaque,
        table_name: []const u8,
        namespace: []const u8,
        created_at_ns: u64,
        policy: catalog_types.NamespacePolicy,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.ensureTable(table_name, namespace, created_at_ns, policy, schema_json, read_schema_json, indexes_json);
    }

    fn erasedListNamespacesAlloc(ptr: *anyopaque, alloc: std.mem.Allocator) ![]catalog_types.NamespaceRecord {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.listNamespacesAlloc(alloc);
    }

    fn erasedListTablesAlloc(ptr: *anyopaque, alloc: std.mem.Allocator) ![]catalog_types.TableNamespaceRecord {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.listTablesAlloc(alloc);
    }

    fn erasedGetTableAlloc(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) !?catalog_types.TableNamespaceRecord {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.getTableAlloc(alloc, table_name);
    }

    fn erasedSetTableDefinition(
        ptr: *anyopaque,
        table_name: []const u8,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.setTableDefinition(table_name, schema_json, read_schema_json, indexes_json);
    }

    fn erasedResolveNamespaceAlloc(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) ![]u8 {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.resolveNamespaceAlloc(alloc, table_name);
    }

    fn erasedGetPolicy(ptr: *anyopaque, namespace: []const u8) !catalog_types.NamespacePolicy {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.getPolicy(namespace);
    }

    fn erasedSetPolicy(ptr: *anyopaque, namespace: []const u8, policy: catalog_types.NamespacePolicy) !catalog_types.NamespacePolicy {
        const self: *ObjectStore = @ptrCast(@alignCast(ptr));
        return try self.setPolicy(namespace, policy);
    }
};

fn stateKeyAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    if (prefix.len == 0) return try alloc.dupe(u8, "namespaces.json");
    return try std.fmt.allocPrint(alloc, "{s}/namespaces.json", .{prefix});
}

fn tableBindingsKeyAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    if (prefix.len == 0) return try alloc.dupe(u8, "tables.json");
    return try std.fmt.allocPrint(alloc, "{s}/tables.json", .{prefix});
}

fn encodeStateAlloc(alloc: std.mem.Allocator, records: []const catalog_types.NamespaceRecord) ![]u8 {
    const persisted = try alloc.alloc(PersistedNamespace, records.len);
    defer alloc.free(persisted);
    for (records, 0..) |record, idx| {
        persisted[idx] = .{
            .name = record.name,
            .created_at_ns = record.created_at_ns,
            .policy = record.policy,
        };
    }
    return try std.json.Stringify.valueAlloc(alloc, persisted, .{});
}

fn encodeTableBindingsAlloc(alloc: std.mem.Allocator, bindings: []const TableBinding) ![]u8 {
    const persisted = try alloc.alloc(PersistedTableBinding, bindings.len);
    defer alloc.free(persisted);
    for (bindings, 0..) |binding, idx| {
        persisted[idx] = .{
            .table_name = binding.table_name,
            .namespace = binding.namespace,
            .schema_json = binding.schema_json,
            .read_schema_json = binding.read_schema_json,
            .indexes_json = binding.indexes_json,
        };
    }
    return try std.json.Stringify.valueAlloc(alloc, persisted, .{});
}

fn decodeStateAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]catalog_types.NamespaceRecord {
    if (std.mem.trim(u8, raw, &std.ascii.whitespace).len == 0) return try alloc.alloc(catalog_types.NamespaceRecord, 0);

    var parsed = try std.json.parseFromSlice([]PersistedNamespace, alloc, raw, .{});
    defer parsed.deinit();

    const out = try alloc.alloc(catalog_types.NamespaceRecord, parsed.value.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer freeNamespaceRecords(alloc, out[0..initialized]);

    for (parsed.value, 0..) |record, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, record.name),
            .created_at_ns = record.created_at_ns,
            .policy = record.policy,
        };
        initialized += 1;
    }
    return out;
}

fn decodeTableBindingsAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]TableBinding {
    if (std.mem.trim(u8, raw, &std.ascii.whitespace).len == 0) return try alloc.alloc(TableBinding, 0);

    var parsed = try std.json.parseFromSlice([]PersistedTableBinding, alloc, raw, .{});
    defer parsed.deinit();

    const out = try alloc.alloc(TableBinding, parsed.value.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer freeTableBindings(alloc, out[0..initialized]);

    for (parsed.value, 0..) |binding, idx| {
        out[idx] = .{
            .table_name = try alloc.dupe(u8, binding.table_name),
            .namespace = try alloc.dupe(u8, binding.namespace),
            .schema_json = try alloc.dupe(u8, binding.schema_json),
            .read_schema_json = try alloc.dupe(u8, binding.read_schema_json),
            .indexes_json = try alloc.dupe(u8, binding.indexes_json),
        };
        initialized += 1;
    }
    return out;
}

fn cloneNamespaceRecordsAlloc(alloc: std.mem.Allocator, records: []const catalog_types.NamespaceRecord) ![]catalog_types.NamespaceRecord {
    const out = try alloc.alloc(catalog_types.NamespaceRecord, records.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer freeNamespaceRecords(alloc, out[0..initialized]);
    for (records, 0..) |record, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, record.name),
            .created_at_ns = record.created_at_ns,
            .policy = record.policy,
        };
        initialized += 1;
    }
    return out;
}

fn freeNamespaceRecords(alloc: std.mem.Allocator, records: []catalog_types.NamespaceRecord) void {
    for (records) |*record| record.deinit(alloc);
    alloc.free(records);
}

fn freeTableBindings(alloc: std.mem.Allocator, bindings: []TableBinding) void {
    for (bindings) |*binding| binding.deinit(alloc);
    alloc.free(bindings);
}

fn lessNamespaceRecord(_: void, lhs: catalog_types.NamespaceRecord, rhs: catalog_types.NamespaceRecord) bool {
    return std.mem.order(u8, lhs.name, rhs.name) == .lt;
}

fn lessTableRecord(_: void, lhs: catalog_types.TableNamespaceRecord, rhs: catalog_types.TableNamespaceRecord) bool {
    return std.mem.order(u8, lhs.table_name, rhs.table_name) == .lt;
}

fn findNamespaceRecord(records: []const catalog_types.NamespaceRecord, namespace: []const u8) ?catalog_types.NamespaceRecord {
    for (records) |record| {
        if (std.mem.eql(u8, record.name, namespace)) return record;
    }
    return null;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

test "objectstore-backed catalog store persists namespace records over file uri" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "catalog");
    defer cleanupTmp(path);

    const uri = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{std.mem.span(path)});
    defer std.testing.allocator.free(uri);

    {
        var impl = try ObjectStore.initFileUri(std.testing.allocator, uri);
        var store = impl.catalogStore();
        defer store.deinit();
        try std.testing.expect(try store.ensureNamespace("docs", 100, .{
            .default_query_view = .latest,
            .keep_latest_versions = 3,
        }));
    }

    {
        var impl = try ObjectStore.initFileUri(std.testing.allocator, uri);
        var store = impl.catalogStore();
        defer store.deinit();
        const records = try store.listNamespacesAlloc(std.testing.allocator);
        defer freeNamespaceRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expectEqualStrings("docs", records[0].name);
    }
}

test "objectstore-backed catalog store persists table bindings over file uri" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "catalog-tables");
    defer cleanupTmp(path);

    const uri = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{std.mem.span(path)});
    defer std.testing.allocator.free(uri);

    {
        var impl = try ObjectStore.initFileUri(std.testing.allocator, uri);
        var store = impl.catalogStore();
        defer store.deinit();
        try std.testing.expect(try store.ensureTable("docs", "docs-serving", 100, .{
            .default_query_view = .latest,
        }, "", "", "{}"));
    }

    {
        var impl = try ObjectStore.initFileUri(std.testing.allocator, uri);
        var store = impl.catalogStore();
        defer store.deinit();
        const tables = try store.listTablesAlloc(std.testing.allocator);
        defer {
            for (tables) |*record| record.deinit(std.testing.allocator);
            std.testing.allocator.free(tables);
        }
        try std.testing.expectEqual(@as(usize, 1), tables.len);
        try std.testing.expectEqualStrings("docs", tables[0].table_name);
        try std.testing.expectEqualStrings("docs-serving", tables[0].namespace);

        const namespace = try store.resolveNamespaceAlloc(std.testing.allocator, "docs");
        defer std.testing.allocator.free(namespace);
        try std.testing.expectEqualStrings("docs-serving", namespace);
    }
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-object-catalog-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
