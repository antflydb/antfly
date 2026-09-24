// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).
//! Disposable source-location hints. CURRENT and immutable payload identities
//! remain authoritative. Queries never allocate while holding this mutex.
const std = @import("std");
const lsm = @import("lsm_backend/mod.zig");
const publication = @import("generation_publication.zig");

pub const Directory = struct {
    pub const Location = struct { generation: u64, shard: u32 };
    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMap([32]u8, Location),
    generation: u64 = 0,
    bytes_written: u64 = 0,
    dirty_entries: u64 = 0,
    publications: u64 = 0,
    publication_deferrals: u64 = 0,
    hits: std.atomic.Value(u64) = .init(0),
    misses: std.atomic.Value(u64) = .init(0),

    pub fn create(alloc: std.mem.Allocator) !*Directory {
        const self = try alloc.create(Directory);
        self.* = .{ .alloc = alloc, .entries = .init(alloc) };
        return self;
    }
    pub fn deinit(self: *Directory) void {
        self.entries.deinit();
        self.alloc.destroy(self);
    }
    fn lock(self: *Directory) void {
        while (!self.mutex.tryLock()) std.Thread.yield() catch {};
    }
    pub fn get(self: *Directory, digest: []const u8) ?Location {
        if (digest.len != 32 or !self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        const found = self.entries.get(digest[0..32].*);
        _ = if (found != null) self.hits.fetchAdd(1, .monotonic) else self.misses.fetchAdd(1, .monotonic);
        return found;
    }
    /// Only the serialized source writer calls maintenance methods. Old
    /// snapshots reject hints naming segments absent from their own manifest.
    pub fn update(self: *Directory, opened: anytype, rebuild: bool) !void {
        self.lock();
        defer self.mutex.unlock();
        if (rebuild) {
            self.entries.clearRetainingCapacity();
            self.generation = 0;
        }
        for (opened.readers) |reader| {
            if (reader.generation <= self.generation) continue;
            for (0..reader.count) |i| {
                const row = reader.sourceIdentityAt(i);
                if (row.key.len != 32 or !row.vector) continue;
                try self.entries.put(row.key[0..32].*, .{ .generation = reader.generation, .shard = reader.shard_id });
                self.dirty_entries += 1;
            }
        }
        self.generation = opened.store.manifest.?.latest_generation;
    }
    /// Remove hints only when they still point at the retired segment.
    /// Newer duplicate payload locations must survive retirement of an old run.
    pub fn removeRetired(self: *Directory, previous: anytype, next: anytype) !void {
        self.lock();
        defer self.mutex.unlock();
        for (previous.readers) |reader| {
            var retained = false;
            for (next.readers) |current| {
                if (current.generation == reader.generation and current.shard_id == reader.shard_id) {
                    retained = true;
                    break;
                }
            }
            if (retained) continue;
            for (0..reader.count) |i| {
                const row = reader.sourceIdentityAt(i);
                if (row.key.len != 32) continue;
                const location = self.entries.get(row.key[0..32].*) orelse continue;
                if (location.generation == reader.generation and location.shard == reader.shard_id) {
                    _ = self.entries.remove(row.key[0..32].*);
                    self.dirty_entries += 1;
                }
            }
        }
    }

    /// CURRENT is the authority. Persist this restart hint after substantial
    /// change, amortizing a full snapshot over at least a quarter of its rows.
    /// A missing or stale snapshot falls back to the authoritative segments.
    pub fn saveCoalesced(self: *Directory, storage: lsm.Storage, root: []const u8) !void {
        if (self.dirty_entries < @max(4096, self.entries.count() / 4)) {
            self.publication_deferrals += 1;
            return;
        }
        try self.save(storage, root);
    }

    pub fn load(self: *Directory, storage: lsm.Storage, root: []const u8) !void {
        const path = try std.fs.path.join(self.alloc, &.{ root, "SOURCE_DIRECTORY" });
        defer self.alloc.free(path);
        const bytes = try storage.readFileAlloc(self.alloc, path, 256 * 1024 * 1024);
        defer self.alloc.free(bytes);
        if (bytes.len < 48 or !std.mem.eql(u8, bytes[0..8], "AFVSDR01") or (bytes.len - 48) % 44 != 0) return error.InvalidSourceDirectory;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[40..], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[8..40])) return error.InvalidSourceDirectory;
        var pos: usize = 48;
        while (pos < bytes.len) : (pos += 44) try self.entries.put(bytes[pos..][0..32].*, .{
            .generation = std.mem.readInt(u64, bytes[pos + 32 ..][0..8], .little),
            .shard = std.mem.readInt(u32, bytes[pos + 40 ..][0..4], .little),
        });
        self.generation = std.mem.readInt(u64, bytes[40..48], .little);
    }
    pub fn save(self: *Directory, storage: lsm.Storage, root: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();
        const bytes = try self.alloc.alloc(u8, 48 + @as(usize, self.entries.count()) * 44);
        defer self.alloc.free(bytes);
        @memcpy(bytes[0..8], "AFVSDR01");
        std.mem.writeInt(u64, bytes[40..48], self.generation, .little);
        var it = self.entries.iterator();
        var pos: usize = 48;
        while (it.next()) |row| : (pos += 44) {
            @memcpy(bytes[pos..][0..32], &row.key_ptr.*);
            std.mem.writeInt(u64, bytes[pos + 32 ..][0..8], row.value_ptr.generation, .little);
            std.mem.writeInt(u32, bytes[pos + 40 ..][0..4], row.value_ptr.shard, .little);
        }
        std.crypto.hash.sha2.Sha256.hash(bytes[40..], bytes[8..40], .{});
        const path = try std.fs.path.join(self.alloc, &.{ root, "SOURCE_DIRECTORY" });
        defer self.alloc.free(path);
        try publication.publishControlFile(self.alloc, storage, path, bytes);
        self.bytes_written += bytes.len;
        self.publications += 1;
        self.dirty_entries = 0;
    }
};
