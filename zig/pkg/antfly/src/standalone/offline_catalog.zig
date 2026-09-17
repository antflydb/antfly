// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Exclusive offline access to the same catalog rows used by standalone.
//! Only the selected table and epoch change; resources, extensions, range and
//! listing indexes remain in the original transactionally durable store.
const std = @import("std");
const lsm = @import("../storage/lsm_backend.zig");
const erased = @import("../storage/backend_erased.zig");
const files = @import("../common/migration_files.zig");
const domain = @import("../system_catalog/domain.zig");
const format = @import("catalog_format.zig");

pub const Catalog = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    lock: std.Io.File,
    backend: lsm.BackendHandle,
    store: erased.Store,
    document: std.json.Parsed(std.json.Value),
    head: ?std.json.Parsed(std.json.Value),

    pub fn open(alloc: std.mem.Allocator, io: std.Io, path: []const u8) !Catalog {
        const lock = try files.lockCatalog(alloc, io, path);
        errdefer lock.close(io);
        const root = try std.fmt.allocPrint(alloc, "{s}.store", .{path});
        defer alloc.free(root);
        var backend = try lsm.BackendHandle.open(alloc, root, .{ .wal_sync_on_commit = true });
        errdefer backend.close();
        var store = try backend.backend.runtimeStore(alloc, .{ .name = "system/metadata" });
        errdefer store.deinit();
        var txn = try store.beginRead();
        defer txn.abort();
        const head_bytes = txn.get(format.head_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (head_bytes) |raw| {
            var decoded = try std.json.parseFromSlice(format.Head, alloc, raw, .{});
            defer decoded.deinit();
            if (decoded.value.version != 1 or decoded.value.epoch == 0 or decoded.value.next_id < 3) return error.InvalidCatalogRecord;
            var head = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
            errdefer head.deinit();
            var document = try std.json.parseFromSlice(std.json.Value, alloc, "{\"tables\":[],\"ranges\":[],\"system_catalog\":{\"resources\":[]}}", .{});
            errdefer document.deinit();
            const a = document.arena.allocator();
            var cursor = try txn.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(format.row_prefix);
            while (entry) |kv| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, kv.key, format.row_prefix)) break;
                const row = try std.json.parseFromSliceLeaky(std.json.Value, a, kv.value, .{ .allocate = .alloc_always });
                if (row != .object) return error.InvalidCatalogRecord;
                if (row.object.get("resource")) |resource| {
                    try document.value.object.getPtr("system_catalog").?.object.getPtr("resources").?.array.append(resource);
                }
                inline for (.{ .{ "table", "tables", "table_id" }, .{ "range", "ranges", "group_id" } }) |kind| {
                    if (row.object.get(kind[0])) |value| {
                        if (value != .object) return error.InvalidCatalogRecord;
                        const id = value.object.get(kind[2]) orelse return error.InvalidCatalogRecord;
                        const encoded = try std.json.Stringify.valueAlloc(a, id, .{});
                        const expected = try std.fmt.allocPrint(a, format.row_prefix ++ "{s}/{s}", .{ kind[0], encoded });
                        if (!std.mem.eql(u8, expected, kv.key)) return error.InvalidCatalogRecord;
                        try document.value.object.getPtr(kind[1]).?.array.append(value);
                    }
                }
            }
            return .{ .alloc = alloc, .io = io, .path = path, .lock = lock, .backend = backend, .store = store, .document = document, .head = head };
        }
        // Import compatibility with standalone checkpoints from main and
        // logical restore seeds. A row store with no head must be empty.
        var cursor = try txn.openCursor();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(format.row_prefix)) |row| {
            if (std.mem.startsWith(u8, row.key, format.row_prefix)) return error.InvalidCatalogRecord;
        }
        const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(64 * 1024 * 1024));
        defer alloc.free(raw);
        const document = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
        return .{ .alloc = alloc, .io = io, .path = path, .lock = lock, .backend = backend, .store = store, .document = document, .head = null };
    }

    pub fn deinit(self: *Catalog) void {
        self.document.deinit();
        if (self.head) |*head| head.deinit();
        self.store.deinit();
        self.backend.close();
        self.lock.close(self.io);
    }

    pub fn findTable(self: *Catalog, name: []const u8) !*std.json.Value {
        const target = try domain.Target.literal(name);
        var state = try std.json.parseFromValue(domain.State, self.alloc, self.document.value.object.get("system_catalog") orelse .{ .object = .{} }, .{ .ignore_unknown_fields = true });
        defer state.deinit();
        var index = try domain.StateIndex.init(self.alloc, state.value);
        defer index.deinit(self.alloc);
        const namespace = try index.namespaceFor(target.database, target.namespace);
        const binding = index.find(.table, namespace.id, target.table);
        const tables = self.document.value.object.getPtr("tables") orelse return error.InvalidCatalogRecord;
        for (tables.array.items) |*table| {
            var decoded = try std.json.parseFromValue(@import("../metadata/table_manager.zig").TableRecord, self.alloc, table.*, .{ .ignore_unknown_fields = true });
            defer decoded.deinit();
            if (binding) |bound| {
                if (decoded.value.table_id != bound.id) continue;
                if (!std.mem.eql(u8, decoded.value.name, bound.storage_name)) return error.InvalidCatalogRecord;
                return table;
            }
            if (std.mem.eql(u8, decoded.value.name, target.table) and index.byId(.table, decoded.value.table_id) == null) return table;
        }
        return error.TableNotFound;
    }

    pub fn publish(self: *Catalog, table: std.json.Value) !void {
        if (self.head) |*head| {
            const a = head.arena.allocator();
            const epoch = head.value.object.getPtr("epoch") orelse return error.InvalidCatalogRecord;
            const previous = try std.json.parseFromValueLeaky(u64, a, epoch.*, .{});
            epoch.* = .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{try std.math.add(u64, previous, 1)}) };
            const id = table.object.get("table_id") orelse return error.InvalidCatalogRecord;
            const encoded_id = try std.json.Stringify.valueAlloc(a, id, .{});
            const key = try std.fmt.allocPrint(a, format.row_prefix ++ "table/{s}", .{encoded_id});
            const row = try std.json.Stringify.valueAlloc(a, .{ .table = table }, .{});
            const header = try std.json.Stringify.valueAlloc(a, head.value, .{});
            var txn = try self.store.beginWrite();
            var open_txn = true;
            defer if (open_txn) txn.abort();
            try txn.put(key, row);
            try txn.put(format.head_key, header);
            try txn.commit();
            open_txn = false;
        } else {
            const a = self.document.arena.allocator();
            const epoch = self.document.value.object.get("epoch") orelse std.json.Value{ .integer = 0 };
            try self.document.value.object.put(a, "epoch", .{ .integer = try std.math.add(i64, epoch.integer, 1) });
            const encoded = try std.json.Stringify.valueAlloc(a, self.document.value, .{});
            try files.writeAtomic(a, self.io, self.path, encoded);
        }
    }
};
