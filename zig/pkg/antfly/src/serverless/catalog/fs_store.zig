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
const fs_paths = @import("../../common/fs_paths.zig");
const catalog_types = @import("types.zig");
const catalog_store = @import("store.zig");

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

    fn deinit(self: *TableBinding, alloc: Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.namespace);
        alloc.free(self.schema_json);
        alloc.free(self.read_schema_json);
        alloc.free(self.indexes_json);
        self.* = undefined;
    }
};

pub const FsStore = struct {
    alloc: Allocator,
    root_dir: []u8,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(alloc: Allocator, root_dir: []const u8) !FsStore {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
        };
    }

    pub fn deinit(self: *FsStore) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn catalogStore(self: *FsStore) catalog_store.CatalogStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn ensureNamespace(self: *FsStore, name: []const u8, created_at_ns: u64, policy: catalog_types.NamespacePolicy) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const records = try self.loadStateAlloc(self.alloc);
        defer freeNamespaceRecords(self.alloc, records);

        for (records) |record| {
            if (std.mem.eql(u8, record.name, name)) return false;
        }

        const updated = try self.alloc.alloc(catalog_types.NamespaceRecord, records.len + 1);
        errdefer self.alloc.free(updated);
        var initialized: usize = 0;
        errdefer freeNamespaceRecords(self.alloc, updated[0..initialized]);

        for (records, 0..) |record, idx| {
            updated[idx] = .{
                .name = try self.alloc.dupe(u8, record.name),
                .created_at_ns = record.created_at_ns,
                .policy = record.policy,
            };
            initialized += 1;
        }

        updated[records.len] = .{
            .name = try self.alloc.dupe(u8, name),
            .created_at_ns = created_at_ns,
            .policy = policy,
        };
        initialized += 1;

        try self.saveState(updated);
        freeNamespaceRecords(self.alloc, updated);
        return true;
    }

    pub fn ensureTable(
        self: *FsStore,
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

        const bindings = try self.loadTableBindingsAlloc(self.alloc);
        defer freeTableBindings(self.alloc, bindings);

        for (bindings) |binding| {
            if (std.mem.eql(u8, binding.table_name, table_name)) return false;
            if (std.mem.eql(u8, binding.namespace, namespace)) return error.NamespaceAlreadyMapped;
        }

        const namespaces = try self.loadStateAlloc(self.alloc);
        defer freeNamespaceRecords(self.alloc, namespaces);

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

            try self.saveState(updated_namespaces);
            freeNamespaceRecords(self.alloc, updated_namespaces);
        }

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

        try self.saveTableBindings(updated_bindings);
        freeTableBindings(self.alloc, updated_bindings);
        return true;
    }

    pub fn listNamespacesAlloc(self: *FsStore, alloc: Allocator) ![]catalog_types.NamespaceRecord {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const records = try self.loadStateAlloc(alloc);
        std.mem.sort(catalog_types.NamespaceRecord, records, {}, lessNamespaceRecord);
        return records;
    }

    pub fn listTablesAlloc(self: *FsStore, alloc: Allocator) ![]catalog_types.TableNamespaceRecord {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const namespaces = try self.loadStateAlloc(alloc);
        defer freeNamespaceRecords(alloc, namespaces);
        const bindings = try self.loadTableBindingsAlloc(alloc);
        defer freeTableBindings(alloc, bindings);

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

    pub fn getTableAlloc(self: *FsStore, alloc: Allocator, table_name: []const u8) !?catalog_types.TableNamespaceRecord {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const namespaces = try self.loadStateAlloc(alloc);
        defer freeNamespaceRecords(alloc, namespaces);
        const bindings = try self.loadTableBindingsAlloc(alloc);
        defer freeTableBindings(alloc, bindings);

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
        self: *FsStore,
        table_name: []const u8,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const bindings = try self.loadTableBindingsAlloc(self.alloc);
        defer freeTableBindings(self.alloc, bindings);

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

        try self.saveTableBindings(updated_bindings);
        freeTableBindings(self.alloc, updated_bindings);
        return true;
    }

    pub fn resolveNamespaceAlloc(self: *FsStore, alloc: Allocator, table_name: []const u8) ![]u8 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const bindings = try self.loadTableBindingsAlloc(alloc);
        defer freeTableBindings(alloc, bindings);

        for (bindings) |binding| {
            if (std.mem.eql(u8, binding.table_name, table_name)) {
                return try alloc.dupe(u8, binding.namespace);
            }
        }
        return error.TableNotFound;
    }

    pub fn getPolicy(self: *FsStore, namespace: []const u8) !catalog_types.NamespacePolicy {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const records = try self.loadStateAlloc(self.alloc);
        defer freeNamespaceRecords(self.alloc, records);

        for (records) |record| {
            if (std.mem.eql(u8, record.name, namespace)) return record.policy;
        }
        return error.NamespaceNotFound;
    }

    pub fn setPolicy(self: *FsStore, namespace: []const u8, policy: catalog_types.NamespacePolicy) !catalog_types.NamespacePolicy {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const records = try self.loadStateAlloc(self.alloc);
        defer freeNamespaceRecords(self.alloc, records);

        var found = false;
        for (records) |*record| {
            if (!std.mem.eql(u8, record.name, namespace)) continue;
            record.policy = policy;
            found = true;
            break;
        }
        if (!found) return error.NamespaceNotFound;
        try self.saveState(records);
        return policy;
    }

    fn loadStateAlloc(self: *FsStore, alloc: Allocator) ![]catalog_types.NamespaceRecord {
        const path = try statePathAlloc(alloc, self.root_dir);
        defer alloc.free(path);
        if (!fileExists(path)) return try alloc.alloc(catalog_types.NamespaceRecord, 0);

        const raw = try readFileAlloc(alloc, path);
        defer alloc.free(raw);
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

    fn saveState(self: *FsStore, records: []const catalog_types.NamespaceRecord) !void {
        const path = try statePathAlloc(self.alloc, self.root_dir);
        defer self.alloc.free(path);
        try ensureParentDir(path);

        const persisted = try self.alloc.alloc(PersistedNamespace, records.len);
        defer self.alloc.free(persisted);
        for (records, 0..) |record, idx| {
            persisted[idx] = .{
                .name = record.name,
                .created_at_ns = record.created_at_ns,
                .policy = record.policy,
            };
        }

        const encoded = try std.json.Stringify.valueAlloc(self.alloc, persisted, .{});
        defer self.alloc.free(encoded);
        try writeFileAtomically(path, encoded);
    }

    fn loadTableBindingsAlloc(self: *FsStore, alloc: Allocator) ![]TableBinding {
        const path = try tableBindingsPathAlloc(alloc, self.root_dir);
        defer alloc.free(path);
        if (!fileExists(path)) return try alloc.alloc(TableBinding, 0);

        const raw = try readFileAlloc(alloc, path);
        defer alloc.free(raw);
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

    fn saveTableBindings(self: *FsStore, bindings: []const TableBinding) !void {
        const path = try tableBindingsPathAlloc(self.alloc, self.root_dir);
        defer self.alloc.free(path);
        try ensureParentDir(path);

        const persisted = try self.alloc.alloc(PersistedTableBinding, bindings.len);
        defer self.alloc.free(persisted);
        for (bindings, 0..) |binding, idx| {
            persisted[idx] = .{
                .table_name = binding.table_name,
                .namespace = binding.namespace,
                .schema_json = binding.schema_json,
                .read_schema_json = binding.read_schema_json,
                .indexes_json = binding.indexes_json,
            };
        }

        const encoded = try std.json.Stringify.valueAlloc(self.alloc, persisted, .{});
        defer self.alloc.free(encoded);
        try writeFileAtomically(path, encoded);
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

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedEnsureNamespace(ptr: *anyopaque, name: []const u8, created_at_ns: u64, policy: catalog_types.NamespacePolicy) !bool {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
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
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.ensureTable(table_name, namespace, created_at_ns, policy, schema_json, read_schema_json, indexes_json);
    }

    fn erasedListNamespacesAlloc(ptr: *anyopaque, alloc: Allocator) ![]catalog_types.NamespaceRecord {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.listNamespacesAlloc(alloc);
    }

    fn erasedListTablesAlloc(ptr: *anyopaque, alloc: Allocator) ![]catalog_types.TableNamespaceRecord {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.listTablesAlloc(alloc);
    }

    fn erasedGetTableAlloc(ptr: *anyopaque, alloc: Allocator, table_name: []const u8) !?catalog_types.TableNamespaceRecord {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getTableAlloc(alloc, table_name);
    }

    fn erasedSetTableDefinition(
        ptr: *anyopaque,
        table_name: []const u8,
        schema_json: []const u8,
        read_schema_json: []const u8,
        indexes_json: []const u8,
    ) !bool {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.setTableDefinition(table_name, schema_json, read_schema_json, indexes_json);
    }

    fn erasedResolveNamespaceAlloc(ptr: *anyopaque, alloc: Allocator, table_name: []const u8) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.resolveNamespaceAlloc(alloc, table_name);
    }

    fn erasedGetPolicy(ptr: *anyopaque, namespace: []const u8) !catalog_types.NamespacePolicy {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getPolicy(namespace);
    }

    fn erasedSetPolicy(ptr: *anyopaque, namespace: []const u8, policy: catalog_types.NamespacePolicy) !catalog_types.NamespacePolicy {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.setPolicy(namespace, policy);
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), parent);
}

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{d}", .{ path, test_nonce.fetchAdd(1, .monotonic) });
    defer std.heap.page_allocator.free(tmp_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    {
        var file = try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true });
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
    }

    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.renameAbsolute(tmp_path, path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    } else {
        std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    }
}

fn statePathAlloc(alloc: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, "namespaces.json" });
}

fn tableBindingsPathAlloc(alloc: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, "tables.json" });
}

fn freeNamespaceRecords(alloc: Allocator, records: []catalog_types.NamespaceRecord) void {
    for (records) |*record| record.deinit(alloc);
    alloc.free(records);
}

fn freeTableBindings(alloc: Allocator, bindings: []TableBinding) void {
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

var test_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-catalog-store-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "fs catalog store persists namespace records and policies" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "persist");
    defer cleanupTmp(path);

    {
        var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
        defer fs.deinit();
        try std.testing.expect(try fs.ensureNamespace("docs", 100, .{
            .default_query_view = .latest,
            .keep_latest_versions = 5,
        }));
        try std.testing.expect(!(try fs.ensureNamespace("docs", 200, .{})));
        const updated = try fs.setPolicy("docs", .{
            .default_query_view = .published,
            .keep_latest_versions = 3,
        });
        try std.testing.expectEqual(@as(usize, 3), updated.keep_latest_versions);
    }

    {
        var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
        defer fs.deinit();
        const policy = try fs.getPolicy("docs");
        try std.testing.expectEqual(catalog_types.DefaultQueryView.published, policy.default_query_view);
        try std.testing.expectEqual(@as(usize, 3), policy.keep_latest_versions);

        const listed = try fs.listNamespacesAlloc(std.testing.allocator);
        defer freeNamespaceRecords(std.testing.allocator, listed);
        try std.testing.expectEqual(@as(usize, 1), listed.len);
        try std.testing.expectEqualStrings("docs", listed[0].name);
        try std.testing.expectEqual(@as(usize, 3), listed[0].policy.keep_latest_versions);
    }
}

test "fs catalog store persists table bindings and resolves serving namespaces" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "table-bindings");
    defer cleanupTmp(path);

    {
        var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
        defer fs.deinit();
        try std.testing.expect(try fs.ensureTable("docs", "docs-serving", 100, .{
            .default_query_view = .latest,
        }, "", "", "{}"));
        try std.testing.expect(!(try fs.ensureTable("docs", "docs-serving", 100, .{}, "", "", "{}")));
    }

    {
        var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
        defer fs.deinit();

        const tables = try fs.listTablesAlloc(std.testing.allocator);
        defer {
            for (tables) |*record| record.deinit(std.testing.allocator);
            std.testing.allocator.free(tables);
        }
        try std.testing.expectEqual(@as(usize, 1), tables.len);
        try std.testing.expectEqualStrings("docs", tables[0].table_name);
        try std.testing.expectEqualStrings("docs-serving", tables[0].namespace);

        const namespace = try fs.resolveNamespaceAlloc(std.testing.allocator, "docs");
        defer std.testing.allocator.free(namespace);
        try std.testing.expectEqualStrings("docs-serving", namespace);
    }
}
