// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable, immutable HTTP prepared resources. The bounded directory is one
//! native atomic index, separate from transaction records and recovery. A
//! successful load owns its copy: close/expiry prevents subsequent admission,
//! but does not cancel an execution that already loaded the resource.
const std = @import("std");
const transactions = @import("transactions.zig");
const catalog = @import("../sql/catalog.zig");
const ast = @import("../sql/ast.zig");
const Budget = @import("../sql/memory_budget.zig");

const key = "\x00sql-prepared-v1";
pub const max_resources = 128;
pub const max_statement_bytes = 64 << 10;
pub const max_record_bytes = 4 << 20;
const max_resource_bytes = 512 << 10;
pub const ttl_ms: u64 = 60 * 60 * 1000;

pub const Binding = struct {
    database: []const u8,
    namespace: []const u8,
    name: []const u8,
    physical_name: []const u8,
    id: u64,
    schema_version: u32,
    revision: u64,

    pub fn from(table: catalog.Table) !Binding {
        const scope = table.scope orelse return error.InvalidSqlBackendResponse;
        return .{ .database = scope.database, .namespace = scope.namespace, .name = scope.name, .physical_name = table.physical_name, .id = table.id, .schema_version = table.schema_version, .revision = scope.revision };
    }

    pub fn verify(bindings: []const Binding, table: catalog.Table) !void {
        const current = try from(table);
        for (bindings) |entry| {
            if (!std.mem.eql(u8, entry.database, current.database) or !std.mem.eql(u8, entry.namespace, current.namespace) or !std.mem.eql(u8, entry.name, current.name)) continue;
            if (entry.id != current.id or entry.schema_version != current.schema_version or entry.revision != current.revision or !std.mem.eql(u8, entry.physical_name, current.physical_name)) return error.CatalogGenerationChanged;
            return;
        }
        return error.CatalogGenerationChanged;
    }
};

pub const Resource = struct {
    id: [32]u8,
    principal: []const u8,
    owner_node_id: u64,
    expires_at_ms: u64,
    database: []const u8,
    namespace: []const u8,
    statement: []const u8,
    parameter_types: []const ?ast.ColumnType,
    bindings: []const Binding,
};

pub const Owned = struct {
    alloc: std.mem.Allocator,
    budget: *Budget,
    parsed: std.json.Parsed(Resource),
    value: Resource,

    pub fn deinit(self: *Owned) void {
        self.parsed.deinit();
        self.alloc.destroy(self.budget);
        self.* = undefined;
    }
};
const Entry = struct { id: [32]u8, expires_at_ms: u64, bytes: usize };
const Directory = struct { resources: []const Entry = &.{} };
const Action = union(enum) { create: Resource, close: []const u8 };

pub fn create(store: *transactions.DurableSessionStore, resource: Resource, now_ms: u64) !void {
    if (resource.statement.len == 0 or resource.statement.len > max_statement_bytes or resource.bindings.len > 64 or resource.parameter_types.len > 1024 or resource.expires_at_ms <= now_ms or resource.expires_at_ms - now_ms > ttl_ms) return error.SqlProgramLimitExceeded;
    _ = try access(store, store.alloc, .{ .create = resource }, resource.principal, resource.owner_node_id, now_ms);
}

pub fn load(store: *transactions.DurableSessionStore, alloc: std.mem.Allocator, id: []const u8, principal: []const u8, owner_node_id: u64, now_ms: u64) !Owned {
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginReadTxn();
            defer txn.abort();
            break :blk try loadTxn(&txn, alloc, id, principal, owner_node_id, now_ms);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginRead();
            defer txn.abort();
            break :blk try loadTxn(&txn, alloc, id, principal, owner_node_id, now_ms);
        },
    };
}

fn resourceKey(id: []const u8) ![key.len + 1 + 32]u8 {
    if (id.len != 32) return error.SqlPreparedNotFound;
    var result: [key.len + 1 + 32]u8 = undefined;
    @memcpy(result[0..key.len], key);
    result[key.len] = '/';
    @memcpy(result[key.len + 1 ..], id);
    return result;
}

fn loadTxn(txn: anytype, alloc: std.mem.Allocator, id: []const u8, principal: []const u8, owner_node_id: u64, now_ms: u64) !Owned {
    const resource_key = try resourceKey(id);
    const raw = txn.get(&resource_key) catch |err| switch (err) {
        error.NotFound => return error.SqlPreparedNotFound,
        else => return err,
    };
    if (raw.len > max_resource_bytes) return error.SqlProgramLimitExceeded;
    const budget = try alloc.create(Budget);
    errdefer alloc.destroy(budget);
    budget.* = .{ .backing = alloc, .limit = 8 << 20 };
    var value = std.json.parseFromSlice(Resource, budget.allocator(), raw, .{ .allocate = .alloc_always }) catch |err| return if (budget.exhausted) error.SqlProgramLimitExceeded else err;
    errdefer value.deinit();
    if (value.value.statement.len > max_statement_bytes or value.value.bindings.len > 64 or value.value.parameter_types.len > 1024) return error.SqlProgramLimitExceeded;
    if (value.value.expires_at_ms <= now_ms or !std.mem.eql(u8, value.value.principal, principal) or !std.mem.eql(u8, &value.value.id, id)) return error.SqlPreparedNotFound;
    if (value.value.owner_node_id != owner_node_id) return error.SqlPreparedWrongOwner;
    return .{ .alloc = alloc, .budget = budget, .parsed = value, .value = value.value };
}

pub fn close(store: *transactions.DurableSessionStore, id: []const u8, principal: []const u8, owner_node_id: u64, now_ms: u64) !void {
    _ = try access(store, store.alloc, .{ .close = id }, principal, owner_node_id, now_ms);
}

fn access(store: *transactions.DurableSessionStore, alloc: std.mem.Allocator, action: Action, principal: []const u8, owner_node_id: u64, now_ms: u64) !void {
    if (store.fail_writes_for_test) return error.InjectedSessionStoreFailure;
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginWriteTxn();
            errdefer txn.abort();
            break :blk try accessTxn(&txn, alloc, action, principal, owner_node_id, now_ms);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginWrite();
            errdefer txn.abort();
            break :blk try accessTxn(&txn, alloc, action, principal, owner_node_id, now_ms);
        },
    };
}

fn accessTxn(txn: anytype, alloc: std.mem.Allocator, action: Action, principal: []const u8, owner_node_id: u64, now_ms: u64) !void {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => "{}",
        else => return err,
    };
    if (raw.len > 64 << 10) return error.SqlProgramLimitExceeded;
    var budget: Budget = .{ .backing = alloc, .limit = 32 << 20 };
    const scratch = budget.allocator();
    var parsed = try std.json.parseFromSlice(Directory, scratch, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.resources.len > max_resources) return error.SqlProgramLimitExceeded;
    var retained: [max_resources]Entry = undefined;
    var count: usize = 0;
    var total_bytes: usize = 0;
    var found = false;
    const id: []const u8 = switch (action) {
        .create => |*value| &value.id,
        .close => |value| value,
    };
    if (action == .close) {
        var owned = try loadTxn(txn, alloc, id, principal, owner_node_id, now_ms);
        owned.deinit();
    }
    for (parsed.value.resources) |resource| {
        if (resource.expires_at_ms <= now_ms) {
            const expired_key = try resourceKey(&resource.id);
            try txn.delete(&expired_key);
            continue;
        }
        if (std.mem.eql(u8, &resource.id, id)) {
            if (action == .create) return error.SqlPreparedAlreadyExists;
            const deleted_key = try resourceKey(id);
            try txn.delete(&deleted_key);
            found = true;
            continue;
        }
        retained[count] = resource;
        total_bytes = try std.math.add(usize, total_bytes, resource.bytes);
        count += 1;
    }
    switch (action) {
        .close => if (!found) return error.SqlPreparedNotFound,
        .create => |resource| {
            if (count == max_resources) return error.SqlWriteCapacityUnavailable;
            const encoded_resource = try std.json.Stringify.valueAlloc(scratch, resource, .{});
            defer scratch.free(encoded_resource);
            if (encoded_resource.len > max_resource_bytes or total_bytes > max_record_bytes -| encoded_resource.len) return error.SqlProgramLimitExceeded;
            const created_key = try resourceKey(&resource.id);
            try txn.put(&created_key, encoded_resource);
            total_bytes += encoded_resource.len;
            retained[count] = .{ .id = resource.id, .expires_at_ms = resource.expires_at_ms, .bytes = encoded_resource.len };
            count += 1;
        },
    }
    const encoded = try std.json.Stringify.valueAlloc(scratch, Directory{ .resources = retained[0..count] }, .{});
    defer scratch.free(encoded);
    if (encoded.len > 64 << 10) return error.SqlProgramLimitExceeded;
    if (total_bytes > max_record_bytes -| encoded.len) return error.SqlProgramLimitExceeded;
    try txn.put(key, encoded);
    try txn.commit();
}

test "SQL prepared durable directory preserves ownership expiry admission and loaded execution" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var native = try backend.runtimeStore(alloc, .{ .name = "prepared-test" });
    defer native.deinit();
    var store = transactions.DurableSessionStore.initRuntime(alloc, &native);
    const first: Resource = .{ .id = @splat('1'), .principal = "alice", .owner_node_id = 7, .expires_at_ms = 100, .database = "app", .namespace = "public", .statement = "SELECT $1::BIGINT", .parameter_types = &.{.integer}, .bindings = &.{} };
    try create(&store, first, 1);
    try std.testing.expectError(error.SqlPreparedNotFound, load(&store, alloc, &first.id, "bob", 7, 2));
    try std.testing.expectError(error.SqlPreparedWrongOwner, load(&store, alloc, &first.id, "alice", 8, 2));
    try std.testing.expectError(error.SqlPreparedNotFound, close(&store, &first.id, "bob", 7, 2));
    try std.testing.expectError(error.SqlPreparedAlreadyExists, create(&store, first, 2));
    var recreated = transactions.DurableSessionStore.initRuntime(alloc, &native);
    var loaded = try load(&recreated, alloc, &first.id, "alice", 7, 2);
    defer loaded.deinit();
    try std.testing.expectEqualStrings(first.statement, loaded.value.statement);
    try std.testing.expectEqual(ast.ColumnType.integer, loaded.value.parameter_types[0].?);
    store.fail_writes_for_test = true;
    var read_during_write_failure = try load(&store, alloc, &first.id, "alice", 7, 2);
    read_during_write_failure.deinit();
    try std.testing.expectError(error.InjectedSessionStoreFailure, close(&store, &first.id, "alice", 7, 2));
    store.fail_writes_for_test = false;
    try close(&store, &first.id, "alice", 7, 2);
    try std.testing.expectError(error.SqlPreparedNotFound, load(&store, alloc, &first.id, "alice", 7, 2));
    // A close does not invalidate a previously admitted request's owned copy.
    try std.testing.expectEqualStrings(first.statement, loaded.value.statement);
    for (0..max_resources) |index| {
        var value = first;
        value.id = std.fmt.bytesToHex(std.mem.toBytes(@as(u128, index)), .lower);
        try create(&store, value, 1);
    }
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, create(&store, first, 2));
    try std.testing.expectError(error.SqlPreparedNotFound, load(&store, alloc, &first.id, "alice", 7, 100));
    var fresh = first;
    fresh.expires_at_ms = 200;
    try create(&store, fresh, 100);
    var after_expiry = try load(&store, alloc, &first.id, "alice", 7, 100);
    defer after_expiry.deinit();
}

test "SQL prepared manifest rejects rebinding and preserves logical aliases" {
    const table: catalog.Table = .{ .id = 9, .physical_name = "physical", .schema_version = 2, .columns = &.{}, .scope = .{ .database = "app", .namespace = "public", .name = "items", .revision = 5 } };
    const binding = try Binding.from(table);
    try Binding.verify(&.{binding}, table);
    var changed = table;
    changed.id += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, Binding.verify(&.{binding}, changed));
    changed = table;
    changed.schema_version += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, Binding.verify(&.{binding}, changed));
    changed = table;
    changed.scope.?.revision += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, Binding.verify(&.{binding}, changed));
    changed = table;
    changed.scope.?.name = "alias";
    try std.testing.expectError(error.CatalogGenerationChanged, Binding.verify(&.{binding}, changed));
    try Binding.verify(&.{ binding, try Binding.from(changed) }, changed);
}

test "SQL prepared resource survives native store restart and transaction cleanup" {
    const alloc = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("sql-prepared-restart");
    defer directory.cleanup();
    const resource: Resource = .{ .id = @splat('a'), .principal = "alice", .owner_node_id = 7, .expires_at_ms = 1000, .database = "app", .namespace = "public", .statement = "SELECT 1", .parameter_types = &.{}, .bindings = &.{} };
    {
        var opened = try transactions.OpenedSessionStore.open(alloc, directory.path());
        defer opened.deinit();
        try create(opened.durableStore(), resource, 1);
        var registry = transactions.SessionRegistry.init(opened.durableStore());
        defer registry.deinit(alloc);
        const transaction = try registry.beginForPrincipal(alloc, .{}, 7, "alice");
        try std.testing.expect(registry.removeBeforeExecution(alloc, transaction.txn_id));
    }
    {
        var reopened = try transactions.OpenedSessionStore.open(alloc, directory.path());
        defer reopened.deinit();
        var value = try load(reopened.durableStore(), alloc, &resource.id, "alice", 7, 2);
        defer value.deinit();
        try std.testing.expectEqualStrings("SELECT 1", value.value.statement);
        try close(reopened.durableStore(), &resource.id, "alice", 7, 2);
    }
    var reopened = try transactions.OpenedSessionStore.open(alloc, directory.path());
    defer reopened.deinit();
    try std.testing.expectError(error.SqlPreparedNotFound, load(reopened.durableStore(), alloc, &resource.id, "alice", 7, 3));
}
