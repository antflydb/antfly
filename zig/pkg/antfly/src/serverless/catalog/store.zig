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
const Allocator = std.mem.Allocator;
const catalog_types = @import("types.zig");

pub const CatalogStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        ensure_namespace: *const fn (*anyopaque, []const u8, u64, catalog_types.NamespacePolicy) anyerror!bool,
        ensure_table: *const fn (*anyopaque, []const u8, []const u8, u64, catalog_types.NamespacePolicy, []const u8, []const u8, []const u8) anyerror!bool,
        list_namespaces_alloc: *const fn (*anyopaque, Allocator) anyerror![]catalog_types.NamespaceRecord,
        list_tables_alloc: *const fn (*anyopaque, Allocator) anyerror![]catalog_types.TableNamespaceRecord,
        get_table_alloc: *const fn (*anyopaque, Allocator, []const u8) anyerror!?catalog_types.TableNamespaceRecord,
        set_table_definition: *const fn (*anyopaque, []const u8, []const u8, []const u8, []const u8) anyerror!bool,
        resolve_namespace_alloc: *const fn (*anyopaque, Allocator, []const u8) anyerror![]u8,
        get_policy: *const fn (*anyopaque, []const u8) anyerror!catalog_types.NamespacePolicy,
        set_policy: *const fn (*anyopaque, []const u8, catalog_types.NamespacePolicy) anyerror!catalog_types.NamespacePolicy,
    };

    pub fn deinit(self: *CatalogStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn ensureNamespace(self: *CatalogStore, name: []const u8, created_at_ns: u64, policy: catalog_types.NamespacePolicy) !bool {
        return try self.vtable.ensure_namespace(self.ptr, name, created_at_ns, policy);
    }

    pub fn ensureTable(
        self: *CatalogStore,
        table_name: []const u8,
        namespace: []const u8,
        created_at_ns: u64,
        policy: catalog_types.NamespacePolicy,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        return try self.vtable.ensure_table(
            self.ptr,
            table_name,
            namespace,
            created_at_ns,
            policy,
            schema_json,
            read_schema_json,
            indexes_json,
        );
    }

    pub fn listNamespacesAlloc(self: *CatalogStore, alloc: Allocator) ![]catalog_types.NamespaceRecord {
        return try self.vtable.list_namespaces_alloc(self.ptr, alloc);
    }

    pub fn listTablesAlloc(self: *CatalogStore, alloc: Allocator) ![]catalog_types.TableNamespaceRecord {
        return try self.vtable.list_tables_alloc(self.ptr, alloc);
    }

    pub fn getTableAlloc(self: *CatalogStore, alloc: Allocator, table_name: []const u8) !?catalog_types.TableNamespaceRecord {
        return try self.vtable.get_table_alloc(self.ptr, alloc, table_name);
    }

    pub fn setTableDefinition(
        self: *CatalogStore,
        table_name: []const u8,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        return try self.vtable.set_table_definition(
            self.ptr,
            table_name,
            schema_json,
            read_schema_json,
            indexes_json,
        );
    }

    pub fn resolveNamespaceAlloc(self: *CatalogStore, alloc: Allocator, table_name: []const u8) ![]u8 {
        return try self.vtable.resolve_namespace_alloc(self.ptr, alloc, table_name);
    }

    pub fn getPolicy(self: *CatalogStore, namespace: []const u8) !catalog_types.NamespacePolicy {
        return try self.vtable.get_policy(self.ptr, namespace);
    }

    pub fn setPolicy(self: *CatalogStore, namespace: []const u8, policy: catalog_types.NamespacePolicy) !catalog_types.NamespacePolicy {
        return try self.vtable.set_policy(self.ptr, namespace, policy);
    }
};
