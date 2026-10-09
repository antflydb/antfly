// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Database-scoped, backend-independent .afb primary images. Derived index
//! files are rebuilt in the unpublished destination, never copied as paths.
//! Capturing all owners together retains local UNIQUE/FK generations and
//! claims without pretending that a single-table transfer owns its parents.
const h = @import("handles.zig");
const std = h.std;
const tables = @import("tables.zig");
const portable = @import("../storage/portable_backup.zig");
const store_mod = @import("../storage/docstore.zig");
const lite_restore = @import("../storage/lite/portable_restore.zig");
const magic = "ANTFLYDBAFB\x00\x01";
const max_tables = 4096;
const catalog_key = "\x00antfly/embedded/tables/v1";

fn imageKey(key: []const u8) bool {
    // Keep the engine's portable primary boundary. Source index checkpoints,
    // inventory certificates, producer leases, and applied watermarks describe
    // the old backend and must be rebuilt against the destination's replay cut.
    return portable.isPortableStoreKey(key) or
        std.mem.startsWith(u8, key, "\x00\x00__metadata__:relational_integrity") or
        std.mem.eql(u8, key, "\x00\x00__metadata__:relational_check_progress") or
        std.mem.startsWith(u8, key, "\x00\x00__metadata__:relational_index_progress:") or
        std.mem.startsWith(u8, key, "\x00\x00__metadata__:document_mutation_revision_v1:") or
        std.mem.startsWith(u8, key, "\x00\x00__columnar__:");
}

fn integer(out: *std.ArrayList(u8), alloc: std.mem.Allocator, comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

pub fn exportDatabase(handle: *h.Handle, out: *std.ArrayList(u8)) !void {
    try tables.requireDatabase(handle);
    try tables.load(handle);
    const count = handle.embedded_tables.count() + 1;
    if (count > max_tables) return error.SqlProgramLimitExceeded;
    const owners = try handle.alloc.alloc(*h.db_mod.DB, count);
    defer handle.alloc.free(owners);
    const names = try handle.alloc.alloc([]const u8, count);
    defer handle.alloc.free(names);
    owners[0] = &handle.db;
    names[0] = "default";
    var iterator = handle.embedded_tables.valueIterator();
    for (owners[1..], names[1..]) |*owner, *name| {
        const table = iterator.next().?.*;
        owner.* = &table.db;
        name.* = table.name;
    }
    // Reserve all native capture leases before exporting any table. API
    // serialization excludes embedded commits; these leases also exclude
    // background mutations. Native intents are excluded from the image.
    const fences = try handle.alloc.alloc(h.db_mod.DB.StatementReadFence, count);
    defer handle.alloc.free(fences);
    var held: usize = 0;
    defer for (fences[0..held]) |*fence| fence.release();
    const io = handle.db.backend_runtime.io() orelse return error.SqlStatementReadUnavailable;
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (held < count) {
        if (try owners[held].tryEmbeddedBackupReadFence()) |fence| {
            fences[held] = fence;
            held += 1;
        } else {
            for (fences[0..held]) |*fence| fence.release();
            held = 0;
            if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.SqlStatementReadUnavailable;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }
    try out.appendSlice(handle.alloc, magic);
    try integer(out, handle.alloc, u32, @intCast(count));
    try integer(out, handle.alloc, u64, handle.embedded_next_table_id);
    for (owners, names) |db, name| {
        var policy = try db.local_execution.row_policy_gate.enterRaw();
        defer policy.release();
        try integer(out, handle.alloc, u32, @intCast(name.len));
        try out.appendSlice(handle.alloc, name);
        try integer(out, handle.alloc, u64, db.core.identity_namespace.table_id);
        try integer(out, handle.alloc, u64, db.core.identity_namespace.shard_id);
        try integer(out, handle.alloc, u64, db.core.identity_namespace.range_id);
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter("");
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!imageKey(kv.key)) continue;
            try integer(out, handle.alloc, u32, @intCast(kv.key.len));
            try integer(out, handle.alloc, u64, @intCast(kv.value.len));
            try out.appendSlice(handle.alloc, kv.key);
            try out.appendSlice(handle.alloc, kv.value);
        }
        try integer(out, handle.alloc, u32, 0);
    }
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(out.items, &digest, .{});
    try out.appendSlice(handle.alloc, &digest);
}

const Reader = struct {
    bytes: []const u8,
    offset: usize = 0,
    fn take(self: *Reader, length: usize) ![]const u8 {
        if (length > self.bytes.len - self.offset) return error.InvalidBackupRequest;
        const result = self.bytes[self.offset..][0..length];
        self.offset += length;
        return result;
    }
    fn int(self: *Reader, comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    pub fn next(self: *Reader) !?store_mod.KVPair {
        const key_len = try self.int(u32);
        if (key_len == 0) return null;
        const value_len = std.math.cast(usize, try self.int(u64)) orelse return error.InvalidBackupRequest;
        return .{ .key = try self.take(key_len), .value = try self.take(value_len) };
    }
};
const Image = struct { name: []const u8, id: u64, shard_id: u64, range_id: u64, entries: []const u8 };
const Archive = struct {
    arena: std.heap.ArenaAllocator,
    next_id: u64,
    images: []const Image,
    fn deinit(self: *Archive) void {
        self.arena.deinit();
    }
};

fn parse(alloc: std.mem.Allocator, bytes: []const u8) !Archive {
    if (bytes.len < magic.len + 12 + 32 or !std.mem.startsWith(u8, bytes, magic)) return error.InvalidBackupRequest;
    const payload = bytes[0 .. bytes.len - 32];
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidBackupRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var reader = Reader{ .bytes = payload, .offset = magic.len };
    const count = try reader.int(u32);
    if (count == 0 or count > max_tables) return error.InvalidBackupRequest;
    const next_id = try reader.int(u64);
    if (next_id < 2) return error.InvalidBackupRequest;
    const images = try owned.alloc(Image, count);
    for (images, 0..) |*image, ordinal| {
        const length = try reader.int(u32);
        if (length == 0 or length > 1024) return error.InvalidBackupRequest;
        const name = try reader.take(length);
        const id = try reader.int(u64);
        const shard_id = try reader.int(u64);
        const range_id = try reader.int(u64);
        if (id == 0 or shard_id == 0 or range_id == 0 or (ordinal != 0 and id >= next_id) or std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name)) return error.InvalidBackupRequest;
        if (ordinal == 0 and !std.mem.eql(u8, name, "default")) return error.InvalidBackupRequest;
        if (ordinal != 0 and (id != shard_id or id != range_id)) return error.InvalidBackupRequest;
        for (images[0..ordinal]) |previous| if (id == previous.id or std.mem.eql(u8, name, previous.name)) return error.InvalidBackupRequest;
        const start = reader.offset;
        var previous: []const u8 = "";
        while (true) {
            const key_len = try reader.int(u32);
            if (key_len == 0) break;
            const value_len = std.math.cast(usize, try reader.int(u64)) orelse return error.InvalidBackupRequest;
            const key = try reader.take(key_len);
            _ = try reader.take(value_len);
            if (!imageKey(key) or std.mem.order(u8, previous, key) != .lt) return error.InvalidBackupRequest;
            previous = key;
        }
        image.* = .{ .name = name, .id = id, .shard_id = shard_id, .range_id = range_id, .entries = payload[start..reader.offset] };
    }
    if (reader.offset != reader.bytes.len) return error.InvalidBackupRequest;
    return .{ .arena = arena, .next_id = next_id, .images = images };
}

pub fn rootIdentity(alloc: std.mem.Allocator, bytes: []const u8) !h.db_mod.DocIdentityNamespace {
    var archive = try parse(alloc, bytes);
    defer archive.deinit();
    const root = archive.images[0];
    return .{ .table_id = root.id, .shard_id = root.shard_id, .range_id = root.range_id };
}

pub fn validate(alloc: std.mem.Allocator, bytes: []const u8) !void {
    var archive = try parse(alloc, bytes);
    defer archive.deinit();
}

/// Populates all namespaces before the caller publishes the enclosing file or
/// directory generation. The supplied root and backend remain caller-owned.
pub fn populate(alloc: std.mem.Allocator, root: *h.db_mod.DB, backend: ?*h.lite_backend.Handle, path: []const u8, bytes: []const u8) !void {
    var archive = try parse(alloc, bytes);
    defer archive.deinit();
    for (archive.images, 0..) |image, ordinal| {
        const identity: @TypeOf(root.core.identity_namespace) = .{ .table_id = image.id, .shard_id = image.shard_id, .range_id = image.range_id };
        var reader = Reader{ .bytes = image.entries };
        if (ordinal == 0) {
            try root.importEmbeddedImageIntoUnpublishedEmpty(alloc, &reader, identity);
            try lite_restore.finalizeRestoredLiteDb(alloc, root);
            continue;
        }
        const namespace = try std.fmt.allocPrint(alloc, "embedded/tables/{d}", .{image.id});
        defer alloc.free(namespace);
        const child_path = if (backend != null) try alloc.dupe(u8, path) else try std.fs.path.join(alloc, &.{ path, namespace });
        defer alloc.free(child_path);
        var opts = h.db_mod.OpenOptions{ .identity_namespace = identity, .prefer_existing_identity_namespace = false, .backend_runtime = root.backend_runtime, .external_derived_checkpoints = false };
        if (backend) |selected| try selected.configureDbOpenOptionsForNamespace(&opts, namespace);
        var db = try h.db_mod.DB.open(alloc, child_path, opts);
        defer db.close();
        try db.importEmbeddedImageIntoUnpublishedEmpty(alloc, &reader, identity);
        try lite_restore.finalizeRestoredLiteDb(alloc, &db);
    }
    // Reconstruct only live catalog entries; dropped physical namespaces and
    // transaction receipts are deliberately outside the portable image.
    const records = try alloc.alloc(struct { name: []const u8, id: u64 }, archive.images.len - 1);
    defer alloc.free(records);
    for (records, archive.images[1..]) |*record, image| record.* = .{ .name = image.name, .id = image.id };
    const catalog = try std.json.Stringify.valueAlloc(alloc, .{ .next_id = archive.next_id, .tables = records }, .{});
    defer alloc.free(catalog);
    var write = try root.core.store.beginWriteTxn();
    errdefer write.abort();
    try write.put(catalog_key, catalog);
    try write.commit();
    try root.sync(true);
}

pub fn importLite(handle: *h.Handle, bytes: []const u8) !void {
    const Populate = struct {
        fn run(backup: []const u8, alloc: std.mem.Allocator, prepared: *lite_restore.LiteDb) !void {
            try populate(alloc, &prepared.db, &prepared.backend, prepared.db.core.path, backup);
        }
    };
    const backend = if (handle.owned_lite_backend) |*selected| selected else return error.InvalidArgument;
    try lite_restore.importPreparedIntoLiteDb(handle.alloc, &handle.db, backend, try rootIdentity(handle.alloc, bytes), bytes, Populate.run);
    handle.embedded_catalog_loaded = false;
}

/// File-backed archive spans avoid an archive-sized allocation during restore.
/// The import heap holds the catalog and one bounded primary write batch.
pub const MappedBackup = struct {
    file: std.Io.File,
    bytes: []align(std.heap.page_size_min) u8,
    pub fn open(io: std.Io, path: []const u8) !MappedBackup {
        const file = try std.Io.Dir.cwd().openFile(io, path, .{ .lock = .shared });
        errdefer file.close(io);
        const stat = try file.stat(io);
        if (stat.size == 0 or stat.size > lite_restore.max_afb_file_bytes) return error.InvalidBackupRequest;
        const length = std.math.cast(usize, stat.size) orelse return error.InvalidBackupRequest;
        const bytes = try std.posix.mmap(null, length, .{ .READ = true }, .{ .TYPE = .PRIVATE }, file.handle, 0);
        return .{ .file = file, .bytes = bytes };
    }
    pub fn deinit(self: *MappedBackup, io: std.Io) void {
        std.posix.munmap(self.bytes);
        self.file.close(io);
    }
};
