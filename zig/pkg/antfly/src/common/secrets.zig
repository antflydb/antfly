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
const platform_sync = @import("antfly_platform").sync;
const builtin = @import("builtin");
const fs_paths = @import("fs_paths.zig");
const platform_time = @import("antfly_platform").time;

const c_env = if (builtin.link_libc and builtin.os.tag != .windows) struct {
    extern "c" var environ: [*:null]?[*:0]u8;
} else struct {};

pub const SecretStatus = enum {
    configured_file,
    configured_env,
    configured_both,
};

pub const ListedSecret = struct {
    key: []u8,
    status: SecretStatus,
    env_var: ?[]u8 = null,
    created_at: ?[]u8 = null,
    updated_at: ?[]u8 = null,

    pub fn deinit(self: *ListedSecret, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        if (self.env_var) |env_var| alloc.free(env_var);
        if (self.created_at) |created_at| alloc.free(created_at);
        if (self.updated_at) |updated_at| alloc.free(updated_at);
        self.* = undefined;
    }
};

pub const SecretValue = union(enum) {
    literal: []u8,
    secret_ref: []u8,
    env_var: []u8,

    pub fn initConfig(alloc: std.mem.Allocator, configured_value: ?[]const u8) !?SecretValue {
        const value = configured_value orelse return null;
        if (parseSecretReference(value)) |key| {
            return .{ .secret_ref = try alloc.dupe(u8, key) };
        }
        return .{ .literal = try alloc.dupe(u8, value) };
    }

    pub fn initConfigOrEnv(alloc: std.mem.Allocator, configured_value: ?[]const u8, env_name: []const u8) !SecretValue {
        if (configured_value) |value| {
            if (parseSecretReference(value)) |key| {
                return .{ .secret_ref = try alloc.dupe(u8, key) };
            }
            return .{ .literal = try alloc.dupe(u8, value) };
        }
        return .{ .env_var = try alloc.dupe(u8, env_name) };
    }

    pub fn deinit(self: *SecretValue, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .literal => |value| alloc.free(value),
            .secret_ref => |value| alloc.free(value),
            .env_var => |value| alloc.free(value),
        }
        self.* = undefined;
    }

    pub fn resolveOwned(self: *const SecretValue, alloc: std.mem.Allocator, secret_store: ?*FileStore) !?[]u8 {
        return switch (self.*) {
            .literal => |value| try alloc.dupe(u8, value),
            .secret_ref => |key| blk: {
                if (secret_store) |store| {
                    break :blk (try store.getOwned(alloc, key)) orelse return error.SecretNotFound;
                }
                const env_var = try envVarForKey(alloc, key);
                defer alloc.free(env_var);
                break :blk envValueOwned(alloc, env_var) orelse return error.SecretNotFound;
            },
            .env_var => |env_var| envValueOwned(alloc, env_var),
        };
    }

    pub fn resolveOwnedWithGeneration(self: *const SecretValue, alloc: std.mem.Allocator, secret_store: ?*FileStore) !ResolvedSecret {
        return switch (self.*) {
            .literal => |value| .{
                .value = try alloc.dupe(u8, value),
                .generation = 0,
                .source = .literal,
            },
            .secret_ref => |key| blk: {
                if (secret_store) |store| {
                    break :blk try store.getOwnedWithGeneration(alloc, key);
                }
                const env_var = try envVarForKey(alloc, key);
                defer alloc.free(env_var);
                const value = envValueOwned(alloc, env_var) orelse return error.SecretNotFound;
                break :blk .{
                    .value = value,
                    .generation = 0,
                    .source = .env_var,
                };
            },
            .env_var => |env_var| blk: {
                const value = envValueOwned(alloc, env_var) orelse return error.SecretNotFound;
                break :blk .{
                    .value = value,
                    .generation = 0,
                    .source = .env_var,
                };
            },
        };
    }

    pub fn identityHash(self: *const SecretValue) u64 {
        return switch (self.*) {
            .literal => |value| std.hash.Wyhash.hash(0, value),
            .secret_ref => |value| std.hash.Wyhash.hash(1, value),
            .env_var => |value| std.hash.Wyhash.hash(2, value),
        };
    }
};

pub const ResolvedSecretSource = enum {
    literal,
    file_store,
    env_var,
};

pub const ResolvedSecret = struct {
    value: []u8,
    generation: u64,
    source: ResolvedSecretSource,

    pub fn deinit(self: *ResolvedSecret, alloc: std.mem.Allocator) void {
        alloc.free(self.value);
        self.* = undefined;
    }

    pub fn cacheGeneration(self: ResolvedSecret) u64 {
        return self.generation;
    }
};

pub const ReloadHealth = struct {
    generation: u64,
    entry_count: usize,
    last_reload_failed: bool,
    stale_snapshot: bool,
    reload_successes: u64,
    reload_failures: u64,
    last_success_ns: u64,
    last_failure_ns: u64,
};

pub const BearerAuthHeaderCache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    generation: u64 = 0,
    header: ?[]u8 = null,

    pub fn deinit(self: *BearerAuthHeaderCache, alloc: std.mem.Allocator) void {
        if (self.header) |value| alloc.free(value);
        self.* = undefined;
    }

    pub fn getOwned(
        self: *BearerAuthHeaderCache,
        cache_alloc: std.mem.Allocator,
        out_alloc: std.mem.Allocator,
        secret: *const SecretValue,
        secret_store: ?*FileStore,
    ) ![]u8 {
        var resolved = try secret.resolveOwnedWithGeneration(out_alloc, secret_store);
        defer resolved.deinit(out_alloc);

        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();

        if (self.header == null or self.generation != resolved.cacheGeneration()) {
            if (self.header) |value| cache_alloc.free(value);
            self.header = try std.fmt.allocPrint(cache_alloc, "Bearer {s}", .{resolved.value});
            self.generation = resolved.cacheGeneration();
        }
        return try out_alloc.dupe(u8, self.header.?);
    }
};

const StoredSecret = struct {
    value: []u8,
    created_at_ns: u64,
    updated_at_ns: u64,

    fn deinit(self: *StoredSecret, alloc: std.mem.Allocator) void {
        alloc.free(self.value);
        self.* = undefined;
    }
};

const PersistedSecret = struct {
    key: []const u8,
    value: []const u8,
    created_at_ns: ?u64 = null,
    updated_at_ns: ?u64 = null,
};

const PersistedSecretsFile = struct {
    secrets: []const PersistedSecret,
};

const FileMetadata = struct {
    inode: std.Io.File.INode,
    size: u64,
    mtime_ns: i128,

    fn eql(self: FileMetadata, other: FileMetadata) bool {
        return self.inode == other.inode and self.size == other.size and self.mtime_ns == other.mtime_ns;
    }
};

pub const FileStore = struct {
    alloc: std.mem.Allocator,
    path: []u8,
    fallbacks: []FileStore = &.{},
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.StringArrayHashMapUnmanaged(StoredSecret) = .{},
    observed_metadata: ?FileMetadata = null,
    generation_value: u64 = 0,
    generation_snapshot: std.atomic.Value(u64) = .init(0),
    last_reload_failed: bool = false,
    reload_success_count: u64 = 0,
    reload_failure_count: u64 = 0,
    last_success_ns: u64 = 0,
    last_failure_ns: u64 = 0,
    next_throttled_refresh_ns: std.atomic.Value(u64) = .init(0),

    pub fn init(alloc: std.mem.Allocator, path: []const u8) !FileStore {
        var store = FileStore{
            .alloc = alloc,
            .path = try alloc.dupe(u8, path),
        };
        errdefer store.deinit();
        try store.load();
        return store;
    }

    pub fn initLayered(alloc: std.mem.Allocator, paths: []const []const u8) !FileStore {
        if (paths.len == 0) return error.InvalidArguments;

        var store = try FileStore.init(alloc, paths[0]);
        errdefer store.deinit();

        if (paths.len > 1) {
            store.fallbacks = try alloc.alloc(FileStore, paths.len - 1);
            var initialized: usize = 0;
            errdefer {
                for (store.fallbacks[0..initialized]) |*fallback| fallback.deinit();
                alloc.free(store.fallbacks);
                store.fallbacks = &.{};
            }
            for (paths[1..]) |path| {
                store.fallbacks[initialized] = try FileStore.init(alloc, path);
                initialized += 1;
            }
        }

        return store;
    }

    pub fn deinit(self: *FileStore) void {
        for (self.fallbacks) |*fallback| fallback.deinit();
        if (self.fallbacks.len > 0) self.alloc.free(self.fallbacks);
        deinitEntries(self.alloc, &self.entries);
        self.entries.deinit(self.alloc);
        self.alloc.free(self.path);
        self.* = undefined;
    }

    pub fn generation(self: *FileStore) u64 {
        self.lock();
        defer self.unlock();
        return self.generationLocked();
    }

    pub fn generationFast(self: *FileStore) u64 {
        if (self.fallbacks.len == 0) return self.generation_snapshot.load(.acquire);
        return self.generation();
    }

    pub fn reloadFailed(self: *FileStore) bool {
        self.lock();
        defer self.unlock();
        if (self.last_reload_failed) return true;
        for (self.fallbacks) |*fallback| {
            if (fallback.reloadFailed()) return true;
        }
        return false;
    }

    pub fn healthSnapshot(self: *FileStore) ReloadHealth {
        self.lock();
        defer self.unlock();
        var health = self.healthSnapshotLocked();
        for (self.fallbacks, 1..) |*fallback, index| {
            const fallback_health = fallback.healthSnapshot();
            health.generation +%= fallback_health.generation *% @as(u64, @intCast(index + 1));
            health.entry_count += fallback_health.entry_count;
            health.last_reload_failed = health.last_reload_failed or fallback_health.last_reload_failed;
            health.stale_snapshot = health.stale_snapshot or fallback_health.stale_snapshot;
            health.reload_successes += fallback_health.reload_successes;
            health.reload_failures += fallback_health.reload_failures;
            health.last_success_ns = @max(health.last_success_ns, fallback_health.last_success_ns);
            health.last_failure_ns = @max(health.last_failure_ns, fallback_health.last_failure_ns);
        }
        return health;
    }

    pub fn refreshIfChanged(self: *FileStore) !bool {
        self.lock();
        defer self.unlock();
        var changed = try self.refreshIfChangedLocked();
        for (self.fallbacks) |*fallback| {
            changed = (try fallback.refreshIfChanged()) or changed;
        }
        return changed;
    }

    /// Refresh at most once per interval. The atomic fast path avoids taking
    /// the store lock or issuing a stat syscall on every cache lookup.
    pub fn refreshIfChangedThrottled(self: *FileStore, interval_ns: u64) !bool {
        if (interval_ns == 0) return try self.refreshIfChanged();
        const now_ns = platform_time.monotonicNs();
        if (now_ns < self.next_throttled_refresh_ns.load(.acquire)) return false;

        self.lock();
        defer self.unlock();
        const locked_now_ns = platform_time.monotonicNs();
        if (locked_now_ns < self.next_throttled_refresh_ns.load(.acquire)) return false;
        self.next_throttled_refresh_ns.store(locked_now_ns +| interval_ns, .release);
        errdefer self.next_throttled_refresh_ns.store(0, .release);

        var changed = try self.refreshIfChangedLocked();
        for (self.fallbacks) |*fallback| {
            changed = (try fallback.refreshIfChangedThrottled(interval_ns)) or changed;
        }
        return changed;
    }

    pub fn list(self: *FileStore, alloc: std.mem.Allocator) ![]ListedSecret {
        self.lock();
        defer self.unlock();
        _ = try self.refreshIfChangedLocked();

        var out = std.ArrayList(ListedSecret).empty;
        errdefer {
            for (out.items) |*item| item.deinit(alloc);
            out.deinit(alloc);
        }
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            try out.append(alloc, try describeStored(alloc, entry.key_ptr.*, entry.value_ptr.*));
        }

        for (self.fallbacks) |*fallback| try fallback.appendFileEntriesForList(alloc, &out);

        const env_only = try listEnvironmentSecrets(alloc);
        defer freeListedSecrets(alloc, env_only);
        for (env_only) |item| {
            if (listedSecretsContain(out.items, item.key)) continue;
            try out.append(alloc, .{
                .key = try alloc.dupe(u8, item.key),
                .status = item.status,
                .env_var = if (item.env_var) |env_var| try alloc.dupe(u8, env_var) else null,
                .created_at = null,
                .updated_at = null,
            });
        }

        std.sort.block(ListedSecret, out.items, {}, lessThanListedSecret);
        return try out.toOwnedSlice(alloc);
    }

    pub fn put(self: *FileStore, alloc: std.mem.Allocator, key: []const u8, value: []const u8) !ListedSecret {
        try validateKey(key);
        self.lock();
        defer self.unlock();
        _ = try self.refreshIfChangedLocked();

        var next = try cloneEntries(self.alloc, self.entries);
        errdefer {
            deinitEntries(self.alloc, &next);
            next.deinit(self.alloc);
        }

        const now_ns = nowNs();
        if (next.getPtr(key)) |existing| {
            const new_value = try self.alloc.dupe(u8, value);
            self.alloc.free(existing.value);
            existing.value = new_value;
            existing.updated_at_ns = now_ns;
        } else insert: {
            const new_key = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(new_key);
            const new_value = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(new_value);
            try next.put(self.alloc, new_key, .{
                .value = new_value,
                .created_at_ns = now_ns,
                .updated_at_ns = now_ns,
            });
            break :insert;
        }
        try self.persistEntries(&next);
        try self.replaceEntriesAfterLocalWriteLocked(&next);
        return try self.describeOneLocked(alloc, key);
    }

    pub fn delete(self: *FileStore, key: []const u8) !bool {
        self.lock();
        defer self.unlock();
        _ = try self.refreshIfChangedLocked();

        const index = self.entries.getIndex(key) orelse return false;
        var next = try cloneEntries(self.alloc, self.entries);
        errdefer {
            deinitEntries(self.alloc, &next);
            next.deinit(self.alloc);
        }

        const next_index = next.getIndex(key) orelse return false;
        self.alloc.free(next.keys()[next_index]);
        var stored = next.values()[next_index];
        stored.deinit(self.alloc);
        _ = next.swapRemoveAt(next_index);
        _ = index;
        try self.persistEntries(&next);
        try self.replaceEntriesAfterLocalWriteLocked(&next);
        return true;
    }

    pub fn getOwned(self: *FileStore, alloc: std.mem.Allocator, key: []const u8) !?[]u8 {
        self.lock();
        defer self.unlock();
        _ = try self.refreshIfChangedLocked();

        if (self.entries.get(key)) |stored| return try alloc.dupe(u8, stored.value);
        for (self.fallbacks) |*fallback| {
            if (try fallback.getOwnedFromFilesNoEnv(alloc, key)) |value| return value;
        }
        const env_var = try envVarForKey(alloc, key);
        defer alloc.free(env_var);
        return envValueOwned(alloc, env_var);
    }

    pub fn getOwnedWithGeneration(self: *FileStore, alloc: std.mem.Allocator, key: []const u8) !ResolvedSecret {
        self.lock();
        defer self.unlock();
        _ = try self.refreshIfChangedLocked();

        if (self.entries.get(key)) |stored| {
            return .{
                .value = try alloc.dupe(u8, stored.value),
                .generation = self.generation_value,
                .source = .file_store,
            };
        }
        for (self.fallbacks) |*fallback| {
            if (try fallback.getOwnedWithGenerationFromFilesNoEnv(alloc, key)) |value| {
                return .{
                    .value = value.value,
                    .generation = self.generationLocked(),
                    .source = value.source,
                };
            }
        }
        const env_var = try envVarForKey(alloc, key);
        defer alloc.free(env_var);
        const value = envValueOwned(alloc, env_var) orelse return error.SecretNotFound;
        return .{
            .value = value,
            .generation = self.generation_value,
            .source = .env_var,
        };
    }

    pub fn resolveValueOwned(self: *FileStore, alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
        const key = parseSecretReference(raw) orelse return try alloc.dupe(u8, raw);
        return (try self.getOwned(alloc, key)) orelse return error.SecretNotFound;
    }

    pub fn resolveValueWithGenerationOwned(self: *FileStore, alloc: std.mem.Allocator, raw: []const u8) !ResolvedSecret {
        const key = parseSecretReference(raw) orelse return .{
            .value = try alloc.dupe(u8, raw),
            .generation = 0,
            .source = .literal,
        };
        return try self.getOwnedWithGeneration(alloc, key);
    }

    fn describeOneLocked(self: *FileStore, alloc: std.mem.Allocator, key: []const u8) !ListedSecret {
        const stored = self.entries.get(key) orelse return error.SecretNotFound;
        return try describeStored(alloc, key, stored);
    }

    fn appendFileEntriesForList(self: *FileStore, alloc: std.mem.Allocator, out: *std.ArrayList(ListedSecret)) !void {
        self.lock();
        defer self.unlock();
        _ = try self.refreshIfChangedLocked();

        var it = self.entries.iterator();
        while (it.next()) |entry| {
            if (listedSecretsContain(out.items, entry.key_ptr.*)) continue;
            try out.append(alloc, try describeStored(alloc, entry.key_ptr.*, entry.value_ptr.*));
        }
        for (self.fallbacks) |*fallback| try fallback.appendFileEntriesForList(alloc, out);
    }

    fn getOwnedFromFilesNoEnv(self: *FileStore, alloc: std.mem.Allocator, key: []const u8) !?[]u8 {
        self.lock();
        defer self.unlock();
        _ = try self.refreshIfChangedLocked();

        if (self.entries.get(key)) |stored| return try alloc.dupe(u8, stored.value);
        for (self.fallbacks) |*fallback| {
            if (try fallback.getOwnedFromFilesNoEnv(alloc, key)) |value| return value;
        }
        return null;
    }

    fn getOwnedWithGenerationFromFilesNoEnv(self: *FileStore, alloc: std.mem.Allocator, key: []const u8) !?ResolvedSecret {
        self.lock();
        defer self.unlock();
        _ = try self.refreshIfChangedLocked();

        if (self.entries.get(key)) |stored| {
            return .{
                .value = try alloc.dupe(u8, stored.value),
                .generation = self.generationLocked(),
                .source = .file_store,
            };
        }
        for (self.fallbacks) |*fallback| {
            if (try fallback.getOwnedWithGenerationFromFilesNoEnv(alloc, key)) |value| return value;
        }
        return null;
    }

    fn load(self: *FileStore) !void {
        const metadata = statFileMetadata(self.path) catch |err| switch (err) {
            error.FileNotFound => {
                self.observed_metadata = null;
                self.markReloadHealthyLocked(false);
                return;
            },
            else => return err,
        };
        if (metadata == null) {
            self.observed_metadata = null;
            self.markReloadHealthyLocked(false);
            return;
        }

        var next = try loadEntriesFromFile(self.alloc, self.path);
        errdefer {
            deinitEntries(self.alloc, &next);
            next.deinit(self.alloc);
        }

        deinitEntries(self.alloc, &self.entries);
        self.entries.deinit(self.alloc);
        self.entries = next;
        next = .{};
        self.observed_metadata = metadata;
        self.markReloadHealthyLocked(true);
    }

    fn refreshIfChangedLocked(self: *FileStore) !bool {
        const metadata = statFileMetadata(self.path) catch |err| switch (err) {
            error.FileNotFound => {
                if (self.observed_metadata != null) {
                    const first_failure = !self.last_reload_failed;
                    self.markReloadFailedLocked();
                    if (first_failure) std.log.warn("secret store file missing; keeping last known good snapshot path={s}", .{self.path});
                } else {
                    self.markReloadHealthyLocked(false);
                }
                return false;
            },
            else => return err,
        };
        if (metadata == null) {
            if (self.observed_metadata != null) {
                const first_failure = !self.last_reload_failed;
                self.markReloadFailedLocked();
                if (first_failure) std.log.warn("secret store file missing; keeping last known good snapshot path={s}", .{self.path});
            } else {
                self.markReloadHealthyLocked(false);
            }
            return false;
        }
        if (self.observed_metadata) |observed| {
            if (observed.eql(metadata.?)) {
                return false;
            }
        }

        var next = loadEntriesFromFile(self.alloc, self.path) catch |err| {
            const first_failure = !self.last_reload_failed;
            self.markReloadFailedLocked();
            if (first_failure) std.log.warn("secret store reload failed; keeping last known good snapshot path={s} err={}", .{ self.path, err });
            return false;
        };
        errdefer {
            deinitEntries(self.alloc, &next);
            next.deinit(self.alloc);
        }
        self.replaceEntriesLocked(&next);
        self.observed_metadata = metadata;
        self.generation_value +%= 1;
        self.generation_snapshot.store(self.generation_value, .release);
        self.markReloadHealthyLocked(true);
        return true;
    }

    fn persistEntries(self: *FileStore, entries: *const std.StringArrayHashMapUnmanaged(StoredSecret)) !void {
        const alloc = self.alloc;
        var persisted = try alloc.alloc(PersistedSecret, entries.count());
        defer alloc.free(persisted);

        var it = entries.iterator();
        var index: usize = 0;
        while (it.next()) |entry| {
            persisted[index] = .{
                .key = entry.key_ptr.*,
                .value = entry.value_ptr.value,
                .created_at_ns = entry.value_ptr.created_at_ns,
                .updated_at_ns = entry.value_ptr.updated_at_ns,
            };
            index += 1;
        }
        std.sort.block(PersistedSecret, persisted, {}, lessThanPersistedSecret);

        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{
            std.json.fmt(PersistedSecretsFile{ .secrets = persisted }, .{}),
        });
        defer alloc.free(encoded);

        try ensureParentDir(self.path);
        try writeFileAtomically(self.path, encoded);
    }

    fn replaceEntriesAfterLocalWriteLocked(self: *FileStore, next: *std.StringArrayHashMapUnmanaged(StoredSecret)) !void {
        self.replaceEntriesLocked(next);
        self.observed_metadata = try statFileMetadata(self.path);
        self.generation_value +%= 1;
        self.generation_snapshot.store(self.generation_value, .release);
        self.markReloadHealthyLocked(true);
    }

    fn replaceEntriesLocked(self: *FileStore, next: *std.StringArrayHashMapUnmanaged(StoredSecret)) void {
        deinitEntries(self.alloc, &self.entries);
        self.entries.deinit(self.alloc);
        self.entries = next.*;
        next.* = .{};
    }

    fn generationLocked(self: *FileStore) u64 {
        var out = self.generation_value;
        for (self.fallbacks, 1..) |*fallback, index| {
            out +%= fallback.generation() *% @as(u64, @intCast(index + 1));
        }
        return out;
    }

    fn lock(self: *FileStore) void {
        platform_sync.lockYielding(&self.mutex);
    }

    fn unlock(self: *FileStore) void {
        self.mutex.unlock();
    }

    fn healthSnapshotLocked(self: *FileStore) ReloadHealth {
        return .{
            .generation = self.generation_value,
            .entry_count = self.entries.count(),
            .last_reload_failed = self.last_reload_failed,
            .stale_snapshot = self.last_reload_failed and self.observed_metadata != null,
            .reload_successes = self.reload_success_count,
            .reload_failures = self.reload_failure_count,
            .last_success_ns = self.last_success_ns,
            .last_failure_ns = self.last_failure_ns,
        };
    }

    fn markReloadHealthyLocked(self: *FileStore, count_success: bool) void {
        self.last_reload_failed = false;
        if (count_success) {
            self.reload_success_count +%= 1;
            self.last_success_ns = nowNs();
        }
    }

    fn markReloadFailedLocked(self: *FileStore) void {
        if (!self.last_reload_failed) self.reload_failure_count +%= 1;
        self.last_reload_failed = true;
        self.last_failure_ns = nowNs();
    }
};

fn loadEntriesFromFile(alloc: std.mem.Allocator, path: []const u8) !std.StringArrayHashMapUnmanaged(StoredSecret) {
    const raw = try readFileAlloc(alloc, path);
    defer alloc.free(raw);

    var parsed = try std.json.parseFromSlice(PersistedSecretsFile, alloc, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    var entries: std.StringArrayHashMapUnmanaged(StoredSecret) = .{};
    errdefer {
        deinitEntries(alloc, &entries);
        entries.deinit(alloc);
    }

    for (parsed.value.secrets) |item| {
        try validateKey(item.key);
        if (entries.contains(item.key)) return error.DuplicateSecretKey;
        const key = try alloc.dupe(u8, item.key);
        errdefer alloc.free(key);
        const value = try alloc.dupe(u8, item.value);
        errdefer alloc.free(value);
        try entries.put(alloc, key, .{
            .value = value,
            .created_at_ns = item.created_at_ns orelse 0,
            .updated_at_ns = item.updated_at_ns orelse item.created_at_ns orelse 0,
        });
    }

    return entries;
}

fn cloneEntries(
    alloc: std.mem.Allocator,
    source: std.StringArrayHashMapUnmanaged(StoredSecret),
) !std.StringArrayHashMapUnmanaged(StoredSecret) {
    var out: std.StringArrayHashMapUnmanaged(StoredSecret) = .{};
    errdefer {
        deinitEntries(alloc, &out);
        out.deinit(alloc);
    }

    var it = source.iterator();
    while (it.next()) |entry| {
        const key = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(key);
        const value = try alloc.dupe(u8, entry.value_ptr.value);
        errdefer alloc.free(value);
        try out.put(alloc, key, .{
            .value = value,
            .created_at_ns = entry.value_ptr.created_at_ns,
            .updated_at_ns = entry.value_ptr.updated_at_ns,
        });
    }
    return out;
}

fn deinitEntries(alloc: std.mem.Allocator, entries: *std.StringArrayHashMapUnmanaged(StoredSecret)) void {
    var it = entries.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.key_ptr.*);
        entry.value_ptr.deinit(alloc);
    }
}

fn listedSecretsContain(items: []const ListedSecret, key: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item.key, key)) return true;
    }
    return false;
}

pub fn freeListedSecrets(alloc: std.mem.Allocator, items: []ListedSecret) void {
    for (items) |*item| item.deinit(alloc);
    alloc.free(items);
}

pub fn listEnvironmentSecrets(alloc: std.mem.Allocator) ![]ListedSecret {
    if (comptime (!builtin.link_libc or builtin.os.tag == .windows)) return try alloc.alloc(ListedSecret, 0);

    var out = std.ArrayList(ListedSecret).empty;
    errdefer {
        for (out.items) |*item| item.deinit(alloc);
        out.deinit(alloc);
    }

    var index: usize = 0;
    while (c_env.environ[index]) |entry_z| : (index += 1) {
        const entry = std.mem.span(entry_z);
        const eq = std.mem.indexOfScalar(u8, entry, '=') orelse continue;
        const env_var = entry[0..eq];
        const key = secretKeyForEnvVar(alloc, env_var) orelse continue;
        errdefer alloc.free(key);
        try out.append(alloc, .{
            .key = key,
            .status = .configured_env,
            .env_var = try alloc.dupe(u8, env_var),
        });
    }

    std.sort.block(ListedSecret, out.items, {}, lessThanListedSecret);
    return try out.toOwnedSlice(alloc);
}

pub fn envVarForKey(alloc: std.mem.Allocator, key: []const u8) ![]u8 {
    var out = try alloc.alloc(u8, key.len);
    for (key, 0..) |ch, i| {
        out[i] = switch (ch) {
            'a'...'z' => std.ascii.toUpper(ch),
            'A'...'Z', '0'...'9' => ch,
            '.', '-', ':' => '_',
            else => '_',
        };
    }
    return out;
}

pub fn parseSecretReference(raw: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, raw, "${secret:")) return null;
    if (raw.len < 11) return null;
    if (raw[raw.len - 1] != '}') return null;
    const key = raw[9 .. raw.len - 1];
    if (key.len == 0) return null;
    return key;
}

pub fn resolveReferenceOwned(
    alloc: std.mem.Allocator,
    secret_store: ?*FileStore,
    raw: []const u8,
) ![]u8 {
    const key = parseSecretReference(raw) orelse return try alloc.dupe(u8, raw);
    if (secret_store) |store| return try store.resolveValueOwned(alloc, raw);
    const env_var = try envVarForKey(alloc, key);
    defer alloc.free(env_var);
    return envValueOwned(alloc, env_var) orelse return error.SecretNotFound;
}

pub fn resolveReferenceWithGenerationOwned(
    alloc: std.mem.Allocator,
    secret_store: ?*FileStore,
    raw: []const u8,
) !ResolvedSecret {
    const key = parseSecretReference(raw) orelse return .{
        .value = try alloc.dupe(u8, raw),
        .generation = 0,
        .source = .literal,
    };
    if (secret_store) |store| return try store.getOwnedWithGeneration(alloc, key);
    const env_var = try envVarForKey(alloc, key);
    defer alloc.free(env_var);
    const value = envValueOwned(alloc, env_var) orelse return error.SecretNotFound;
    return .{
        .value = value,
        .generation = 0,
        .source = .env_var,
    };
}

pub fn validateKey(key: []const u8) !void {
    if (key.len == 0) return error.InvalidSecretKey;
    if (key[0] == '.' or key[key.len - 1] == '.') return error.InvalidSecretKey;
    var prev_dot = false;
    for (key) |ch| {
        switch (ch) {
            'a'...'z', 'A'...'Z', '0'...'9', '_', '-', '.' => {},
            else => return error.InvalidSecretKey,
        }
        if (ch == '.') {
            if (prev_dot) return error.InvalidSecretKey;
            prev_dot = true;
        } else {
            prev_dot = false;
        }
    }
}

fn describeStored(alloc: std.mem.Allocator, key: []const u8, stored: StoredSecret) !ListedSecret {
    const env_var = try envVarForKey(alloc, key);
    const has_env = hasEnvVar(env_var);
    return .{
        .key = try alloc.dupe(u8, key),
        .status = if (has_env) .configured_both else .configured_file,
        .env_var = env_var,
        .created_at = if (stored.created_at_ns > 0) try formatTimestampOwned(alloc, stored.created_at_ns) else null,
        .updated_at = if (stored.updated_at_ns > 0) try formatTimestampOwned(alloc, stored.updated_at_ns) else null,
    };
}

fn secretKeyForEnvVar(alloc: std.mem.Allocator, env_var: []const u8) ?[]u8 {
    if (!std.mem.endsWith(u8, env_var, "_API_KEY")) return null;
    if (env_var.len <= "_API_KEY".len) return null;
    const prefix = env_var[0 .. env_var.len - "_API_KEY".len];
    var out = alloc.alloc(u8, prefix.len + ".api_key".len) catch return null;
    var index: usize = 0;
    for (prefix) |ch| {
        switch (ch) {
            'A'...'Z' => {
                out[index] = std.ascii.toLower(ch);
                index += 1;
            },
            '0'...'9' => {
                out[index] = ch;
                index += 1;
            },
            '_' => {
                out[index] = '.';
                index += 1;
            },
            else => {
                alloc.free(out);
                return null;
            },
        }
    }
    @memcpy(out[index .. index + ".api_key".len], ".api_key");
    index += ".api_key".len;
    return out[0..index];
}

fn hasEnvVar(env_var: []const u8) bool {
    if (!builtin.link_libc) return false;
    const env_var_z = std.heap.smp_allocator.dupeZ(u8, env_var) catch return false;
    defer std.heap.smp_allocator.free(env_var_z);
    return std.c.getenv(env_var_z.ptr) != null;
}

fn envValueOwned(alloc: std.mem.Allocator, env_var: []const u8) ?[]u8 {
    if (!builtin.link_libc) return null;
    const env_var_z = alloc.dupeZ(u8, env_var) catch return null;
    defer alloc.free(env_var_z);
    const raw = std.c.getenv(env_var_z.ptr) orelse return null;
    return alloc.dupe(u8, std.mem.span(raw)) catch null;
}

fn formatTimestampOwned(alloc: std.mem.Allocator, ns: u64) ![]u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{
        .secs = @divFloor(ns, std.time.ns_per_s),
    };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
    });
}

fn readFileAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(16 * 1024 * 1024));
}

fn statFileMetadata(path: []const u8) !?FileMetadata {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const stat = if (std.fs.path.isAbsolute(path)) blk: {
        var file = std.Io.Dir.openFileAbsolute(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer file.close(io);
        break :blk try file.stat(io);
    } else std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    return .{
        .inode = stat.inode,
        .size = stat.size,
        .mtime_ns = stat.mtime.toNanoseconds(),
    };
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), parent);
}

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-secrets-{d}", .{ path, nowNs() });
    defer std.heap.page_allocator.free(tmp_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var tmp_exists = false;
    defer if (tmp_exists) deleteFileWithIo(io, tmp_path) catch {};

    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true, .exclusive = true });
        tmp_exists = true;
        defer file.close(io);
        if (builtin.os.tag != .windows and builtin.os.tag != .wasi and builtin.os.tag != .freestanding) {
            try file.setPermissions(io, @enumFromInt(0o600));
        }
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
        try file.sync(io);
    }

    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.renameAbsolute(tmp_path, path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    }
    tmp_exists = false;
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

fn deleteFile(path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try deleteFileWithIo(io_impl.io(), path);
}

fn deleteFileWithIo(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io, path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
}

fn nowNs() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn lessThanListedSecret(_: void, lhs: ListedSecret, rhs: ListedSecret) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

fn lessThanPersistedSecret(_: void, lhs: PersistedSecret, rhs: PersistedSecret) bool {
    return std.mem.order(u8, lhs.key, rhs.key) == .lt;
}

test "file secret store persists values and overlays env status" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-{d}.json", .{nowNs()});
    defer alloc.free(path);
    defer deleteFile(path) catch {};

    var store = try FileStore.init(alloc, path);
    defer store.deinit();

    var entry = try store.put(alloc, "openai.api_key", "abc123");
    defer entry.deinit(alloc);
    try std.testing.expectEqual(SecretStatus.configured_file, entry.status);
    try std.testing.expectEqualStrings("OPENAI_API_KEY", entry.env_var.?);
    if (builtin.os.tag != .windows and builtin.os.tag != .wasi and builtin.os.tag != .freestanding) {
        var file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_only });
        defer file.close(std.testing.io);
        const stat = try file.stat(std.testing.io);
        try std.testing.expectEqual(@as(std.posix.mode_t, 0), stat.permissions.toMode() & 0o077);
    }

    const stored = try store.getOwned(alloc, "openai.api_key");
    defer if (stored) |value| alloc.free(value);
    try std.testing.expectEqualStrings("abc123", stored.?);

    var reloaded = try FileStore.init(alloc, path);
    defer reloaded.deinit();
    const reloaded_value = try reloaded.getOwned(alloc, "openai.api_key");
    defer if (reloaded_value) |value| alloc.free(value);
    try std.testing.expectEqualStrings("abc123", reloaded_value.?);

    const deleted = try reloaded.delete("openai.api_key");
    try std.testing.expect(deleted);
    try std.testing.expectEqual(@as(?[]u8, null), try reloaded.getOwned(alloc, "openai.api_key"));
}

test "file secret store reloads valid external replacements including deletions" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-reload-{d}.json", .{nowNs()});
    defer alloc.free(path);
    defer deleteFile(path) catch {};

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"first","created_at_ns":1,"updated_at_ns":1},{"key":"deleted.dynamic_secret","value":"deleted","created_at_ns":1,"updated_at_ns":1}]}
    );

    var store = try FileStore.init(alloc, path);
    defer store.deinit();
    const initial_generation = store.generation();
    try std.testing.expectEqual(initial_generation, store.generationFast());

    const first = try store.getOwned(alloc, "openai.api_key");
    defer if (first) |value| alloc.free(value);
    try std.testing.expectEqualStrings("first", first.?);

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"second-longer","created_at_ns":1,"updated_at_ns":2}]}
    );

    const second = try store.getOwned(alloc, "openai.api_key");
    defer if (second) |value| alloc.free(value);
    try std.testing.expectEqualStrings("second-longer", second.?);
    try std.testing.expect(store.generation() == initial_generation + 1);

    const deleted = try store.getOwned(alloc, "deleted.dynamic_secret");
    defer if (deleted) |value| alloc.free(value);
    try std.testing.expectEqual(@as(?[]u8, null), deleted);
}

test "file secret store detects projected volume symlink target replacement" {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) {
        return error.SkipZigTest;
    }

    const alloc = std.testing.allocator;
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-projected-{d}", .{nowNs()});
    defer alloc.free(root);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};

    const first_dir = try std.fmt.allocPrint(alloc, "{s}/..2026_01", .{root});
    defer alloc.free(first_dir);
    const second_dir = try std.fmt.allocPrint(alloc, "{s}/..2026_02", .{root});
    defer alloc.free(second_dir);
    try fs_paths.createDirPathPortable(io, first_dir);
    try fs_paths.createDirPathPortable(io, second_dir);

    const first_path = try std.fmt.allocPrint(alloc, "{s}/secrets.json", .{first_dir});
    defer alloc.free(first_path);
    const second_path = try std.fmt.allocPrint(alloc, "{s}/secrets.json", .{second_dir});
    defer alloc.free(second_path);
    const first_json =
        \\{"secrets":[{"key":"openai.api_key","value":"first","created_at_ns":1,"updated_at_ns":1}]}
    ;
    const second_json =
        \\{"secrets":[{"key":"openai.api_key","value":"other","created_at_ns":1,"updated_at_ns":1}]}
    ;
    try std.testing.expectEqual(first_json.len, second_json.len);
    try writeFileAtomically(first_path, first_json);
    try writeFileAtomically(second_path, second_json);

    // Reproduce the hard case for size+mtime detection: projected generations
    // contain same-length values and carry the same modification timestamp.
    const first_stat = try std.Io.Dir.cwd().statFile(io, first_path, .{});
    try std.Io.Dir.cwd().setTimestamps(io, second_path, .{
        .modify_timestamp = .{ .new = first_stat.mtime },
    });

    const data_link = try std.fmt.allocPrint(alloc, "{s}/..data", .{root});
    defer alloc.free(data_link);
    const next_data_link = try std.fmt.allocPrint(alloc, "{s}/..data-next", .{root});
    defer alloc.free(next_data_link);
    const secret_link = try std.fmt.allocPrint(alloc, "{s}/secrets.json", .{root});
    defer alloc.free(secret_link);
    try std.Io.Dir.cwd().symLink(io, "..2026_01", data_link, .{ .is_directory = true });
    try std.Io.Dir.cwd().symLink(io, "..data/secrets.json", secret_link, .{});

    var store = try FileStore.init(alloc, secret_link);
    defer store.deinit();
    const initial_generation = store.generation();
    const initial_metadata = store.observed_metadata.?;

    try std.Io.Dir.cwd().symLink(io, "..2026_02", next_data_link, .{ .is_directory = true });
    try std.Io.Dir.rename(std.Io.Dir.cwd(), next_data_link, std.Io.Dir.cwd(), data_link, io);

    const replacement_metadata = (try statFileMetadata(secret_link)).?;
    try std.testing.expectEqual(initial_metadata.size, replacement_metadata.size);
    try std.testing.expectEqual(initial_metadata.mtime_ns, replacement_metadata.mtime_ns);
    try std.testing.expect(initial_metadata.inode != replacement_metadata.inode);

    const reloaded = try store.getOwned(alloc, "openai.api_key");
    defer if (reloaded) |value| alloc.free(value);
    try std.testing.expectEqualStrings("other", reloaded.?);
    try std.testing.expectEqual(initial_generation + 1, store.generation());
}

test "file secret store throttles cache-key freshness checks" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-throttle-{d}.json", .{nowNs()});
    defer alloc.free(path);
    defer deleteFile(path) catch {};

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"first","created_at_ns":1,"updated_at_ns":1}]}
    );
    var store = try FileStore.init(alloc, path);
    defer store.deinit();
    const initial_generation = store.generation();
    try std.testing.expect(!(try store.refreshIfChangedThrottled(std.time.ns_per_hour)));

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"second-longer","created_at_ns":1,"updated_at_ns":2}]}
    );
    try std.testing.expect(!(try store.refreshIfChangedThrottled(std.time.ns_per_hour)));
    try std.testing.expectEqual(initial_generation, store.generation());

    try std.testing.expect(try store.refreshIfChanged());
    try std.testing.expectEqual(initial_generation + 1, store.generation());
    try std.testing.expectEqual(initial_generation + 1, store.generationFast());
}

test "file secret store keeps last known good snapshot for malformed and missing files" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-bad-reload-{d}.json", .{nowNs()});
    defer alloc.free(path);
    defer deleteFile(path) catch {};

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"stable","created_at_ns":1,"updated_at_ns":1}]}
    );

    var store = try FileStore.init(alloc, path);
    defer store.deinit();

    try writeFileAtomically(path, "{not-json");
    const malformed_generation = store.generation();
    const after_malformed = try store.getOwned(alloc, "openai.api_key");
    defer if (after_malformed) |value| alloc.free(value);
    try std.testing.expectEqualStrings("stable", after_malformed.?);
    try std.testing.expect(store.reloadFailed());
    try std.testing.expectEqual(malformed_generation, store.generation());
    const malformed_health = store.healthSnapshot();
    try std.testing.expect(malformed_health.last_reload_failed);
    try std.testing.expect(malformed_health.stale_snapshot);
    try std.testing.expectEqual(@as(u64, 1), malformed_health.reload_failures);
    try std.testing.expect(malformed_health.last_failure_ns != 0);

    try deleteFile(path);
    const missing_generation = store.generation();
    const after_missing = try store.getOwned(alloc, "openai.api_key");
    defer if (after_missing) |value| alloc.free(value);
    try std.testing.expectEqualStrings("stable", after_missing.?);
    try std.testing.expect(store.reloadFailed());
    try std.testing.expectEqual(missing_generation, store.generation());
    const missing_health = store.healthSnapshot();
    try std.testing.expect(missing_health.last_reload_failed);
    try std.testing.expect(missing_health.stale_snapshot);
    try std.testing.expectEqual(@as(u64, 1), missing_health.reload_failures);
}

test "file secret store write refreshes first and preserves external keys" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-write-refresh-{d}.json", .{nowNs()});
    defer alloc.free(path);
    defer deleteFile(path) catch {};

    var store = try FileStore.init(alloc, path);
    defer store.deinit();

    var entry = try store.put(alloc, "openai.api_key", "first");
    defer entry.deinit(alloc);

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"external","created_at_ns":1,"updated_at_ns":2},{"key":"gemini.api_key","value":"gemini","created_at_ns":1,"updated_at_ns":1}]}
    );

    var updated = try store.put(alloc, "anthropic.api_key", "anthropic");
    defer updated.deinit(alloc);

    const openai = try store.getOwned(alloc, "openai.api_key");
    defer if (openai) |value| alloc.free(value);
    try std.testing.expectEqualStrings("external", openai.?);

    const gemini = try store.getOwned(alloc, "gemini.api_key");
    defer if (gemini) |value| alloc.free(value);
    try std.testing.expectEqualStrings("gemini", gemini.?);

    const anthropic = try store.getOwned(alloc, "anthropic.api_key");
    defer if (anthropic) |value| alloc.free(value);
    try std.testing.expectEqualStrings("anthropic", anthropic.?);
}

test "layered file secret store resolves primary before fallback and writes primary" {
    const alloc = std.testing.allocator;
    const primary_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-layer-primary-{d}.json", .{nowNs()});
    defer alloc.free(primary_path);
    defer deleteFile(primary_path) catch {};
    const fallback_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-layer-fallback-{d}.json", .{nowNs()});
    defer alloc.free(fallback_path);
    defer deleteFile(fallback_path) catch {};

    try writeFileAtomically(primary_path,
        \\{"secrets":[{"key":"openai.api_key","value":"primary-openai","created_at_ns":1,"updated_at_ns":1},{"key":"shared.key","value":"primary-shared","created_at_ns":1,"updated_at_ns":1}]}
    );
    try writeFileAtomically(fallback_path,
        \\{"secrets":[{"key":"antfly.runtime.test.token","value":"fallback-token","created_at_ns":1,"updated_at_ns":1},{"key":"shared.key","value":"fallback-shared","created_at_ns":1,"updated_at_ns":1}]}
    );

    var store = try FileStore.initLayered(alloc, &.{ primary_path, fallback_path });
    defer store.deinit();

    const primary = try store.getOwned(alloc, "openai.api_key");
    defer if (primary) |value| alloc.free(value);
    try std.testing.expectEqualStrings("primary-openai", primary.?);

    const fallback = try store.getOwned(alloc, "antfly.runtime.test.token");
    defer if (fallback) |value| alloc.free(value);
    try std.testing.expectEqualStrings("fallback-token", fallback.?);

    const shared = try store.getOwned(alloc, "shared.key");
    defer if (shared) |value| alloc.free(value);
    try std.testing.expectEqualStrings("primary-shared", shared.?);

    var written = try store.put(alloc, "anthropic.api_key", "primary-write");
    defer written.deinit(alloc);

    var reloaded_primary = try FileStore.init(alloc, primary_path);
    defer reloaded_primary.deinit();
    const primary_write = try reloaded_primary.getOwned(alloc, "anthropic.api_key");
    defer if (primary_write) |value| alloc.free(value);
    try std.testing.expectEqualStrings("primary-write", primary_write.?);

    var reloaded_fallback = try FileStore.init(alloc, fallback_path);
    defer reloaded_fallback.deinit();
    const fallback_write = try reloaded_fallback.getOwned(alloc, "anthropic.api_key");
    defer if (fallback_write) |value| alloc.free(value);
    try std.testing.expectEqual(@as(?[]u8, null), fallback_write);
}

test "layered file secret store generation changes when fallback changes" {
    const alloc = std.testing.allocator;
    const primary_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-layer-generation-primary-{d}.json", .{nowNs()});
    defer alloc.free(primary_path);
    defer deleteFile(primary_path) catch {};
    const fallback_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-layer-generation-fallback-{d}.json", .{nowNs()});
    defer alloc.free(fallback_path);
    defer deleteFile(fallback_path) catch {};

    try writeFileAtomically(primary_path,
        \\{"secrets":[]}
    );
    try writeFileAtomically(fallback_path,
        \\{"secrets":[{"key":"antfly.runtime.test.token","value":"first","created_at_ns":1,"updated_at_ns":1}]}
    );

    var store = try FileStore.initLayered(alloc, &.{ primary_path, fallback_path });
    defer store.deinit();

    var first = try resolveReferenceWithGenerationOwned(alloc, &store, "${secret:antfly.runtime.test.token}");
    defer first.deinit(alloc);
    try std.testing.expectEqualStrings("first", first.value);

    try writeFileAtomically(fallback_path,
        \\{"secrets":[{"key":"antfly.runtime.test.token","value":"second","created_at_ns":1,"updated_at_ns":2}]}
    );

    var second = try resolveReferenceWithGenerationOwned(alloc, &store, "${secret:antfly.runtime.test.token}");
    defer second.deinit(alloc);
    try std.testing.expectEqualStrings("second", second.value);
    try std.testing.expect(second.generation > first.generation);
}

test "parse secret reference extracts key name" {
    try std.testing.expectEqualStrings("pg_dsn", parseSecretReference("${secret:pg_dsn}").?);
    try std.testing.expect(parseSecretReference("plain") == null);
    try std.testing.expect(parseSecretReference("${secret:}") == null);
}

test "secret value resolves file-backed references at request time" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secret-value-reload-{d}.json", .{nowNs()});
    defer alloc.free(path);
    defer deleteFile(path) catch {};

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"first","created_at_ns":1,"updated_at_ns":1}]}
    );

    var store = try FileStore.init(alloc, path);
    defer store.deinit();

    var value = try SecretValue.initConfigOrEnv(alloc, "${secret:openai.api_key}", "OPENAI_API_KEY");
    defer value.deinit(alloc);

    const first = try value.resolveOwned(alloc, &store);
    defer if (first) |resolved| alloc.free(resolved);
    try std.testing.expectEqualStrings("first", first.?);

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"second-longer","created_at_ns":1,"updated_at_ns":2}]}
    );
    const second = try value.resolveOwned(alloc, &store);
    defer if (second) |resolved| alloc.free(resolved);
    try std.testing.expectEqualStrings("second-longer", second.?);

    try writeFileAtomically(path, "{\"secrets\":[]}");
    try std.testing.expectError(error.SecretNotFound, value.resolveOwned(alloc, &store));
}

test "secret resolution reports file generation for cache invalidation" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secret-generation-{d}.json", .{nowNs()});
    defer alloc.free(path);
    defer deleteFile(path) catch {};

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"pg.dsn","value":"first","created_at_ns":1,"updated_at_ns":1}]}
    );

    var store = try FileStore.init(alloc, path);
    defer store.deinit();

    var first = try resolveReferenceWithGenerationOwned(alloc, &store, "${secret:pg.dsn}");
    defer first.deinit(alloc);
    try std.testing.expectEqualStrings("first", first.value);
    try std.testing.expectEqual(ResolvedSecretSource.file_store, first.source);
    try std.testing.expectEqual(store.generation(), first.generation);

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"pg.dsn","value":"second-longer","created_at_ns":1,"updated_at_ns":2}]}
    );

    var second = try resolveReferenceWithGenerationOwned(alloc, &store, "${secret:pg.dsn}");
    defer second.deinit(alloc);
    try std.testing.expectEqualStrings("second-longer", second.value);
    try std.testing.expectEqual(ResolvedSecretSource.file_store, second.source);
    try std.testing.expect(second.generation > first.generation);

    var literal = try resolveReferenceWithGenerationOwned(alloc, &store, "postgres://literal");
    defer literal.deinit(alloc);
    try std.testing.expectEqualStrings("postgres://literal", literal.value);
    try std.testing.expectEqual(ResolvedSecretSource.literal, literal.source);
    try std.testing.expectEqual(@as(u64, 0), literal.generation);
}

test "bearer auth header cache rebuilds on file generation change" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secret-auth-header-generation-{d}.json", .{nowNs()});
    defer alloc.free(path);
    defer deleteFile(path) catch {};

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"first","created_at_ns":1,"updated_at_ns":1}]}
    );

    var store = try FileStore.init(alloc, path);
    defer store.deinit();

    var secret = try SecretValue.initConfig(alloc, "${secret:openai.api_key}") orelse return error.TestUnexpectedResult;
    defer secret.deinit(alloc);

    var cache = BearerAuthHeaderCache{};
    defer cache.deinit(alloc);

    const first = try cache.getOwned(alloc, alloc, &secret, &store);
    defer alloc.free(first);
    try std.testing.expectEqualStrings("Bearer first", first);
    const first_generation = cache.generation;

    const first_again = try cache.getOwned(alloc, alloc, &secret, &store);
    defer alloc.free(first_again);
    try std.testing.expectEqualStrings("Bearer first", first_again);
    try std.testing.expectEqual(first_generation, cache.generation);

    try writeFileAtomically(path,
        \\{"secrets":[{"key":"openai.api_key","value":"second","created_at_ns":1,"updated_at_ns":2}]}
    );

    const second = try cache.getOwned(alloc, alloc, &secret, &store);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("Bearer second", second);
    try std.testing.expect(cache.generation > first_generation);
}

test "environment secret discovery maps API key env vars" {
    const alloc = std.testing.allocator;
    const env_var = try envVarForKey(alloc, "anthropic.api_key");
    defer alloc.free(env_var);
    try std.testing.expectEqualStrings("ANTHROPIC_API_KEY", env_var);
    const key = secretKeyForEnvVar(alloc, "ANTHROPIC_API_KEY").?;
    defer alloc.free(key);
    try std.testing.expectEqualStrings("anthropic.api_key", key);
}
