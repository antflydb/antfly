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
const builtin = @import("builtin");
const fs_paths = @import("fs_paths.zig");

const c_env = if (builtin.link_libc and builtin.os.tag != .windows) struct {
    extern "c" var environ: [*:null]?[*:0]u8;
} else struct {};

pub const SecretStatus = enum {
    configured_keystore,
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
    size: u64,
    mtime_ns: i128,

    fn eql(self: FileMetadata, other: FileMetadata) bool {
        return self.size == other.size and self.mtime_ns == other.mtime_ns;
    }
};

pub const FileStore = struct {
    alloc: std.mem.Allocator,
    path: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.StringArrayHashMapUnmanaged(StoredSecret) = .{},
    observed_metadata: ?FileMetadata = null,
    generation_value: u64 = 0,
    last_reload_failed: bool = false,

    pub fn init(alloc: std.mem.Allocator, path: []const u8) !FileStore {
        var store = FileStore{
            .alloc = alloc,
            .path = try alloc.dupe(u8, path),
        };
        errdefer store.deinit();
        try store.load();
        return store;
    }

    pub fn deinit(self: *FileStore) void {
        deinitEntries(self.alloc, &self.entries);
        self.entries.deinit(self.alloc);
        self.alloc.free(self.path);
        self.* = undefined;
    }

    pub fn generation(self: *FileStore) u64 {
        self.lock();
        defer self.unlock();
        return self.generation_value;
    }

    pub fn reloadFailed(self: *FileStore) bool {
        self.lock();
        defer self.unlock();
        return self.last_reload_failed;
    }

    pub fn refreshIfChanged(self: *FileStore) !bool {
        self.lock();
        defer self.unlock();
        return try self.refreshIfChangedLocked();
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

        const env_only = try listEnvironmentSecrets(alloc);
        defer freeListedSecrets(alloc, env_only);
        for (env_only) |item| {
            if (self.entries.contains(item.key)) continue;
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

        const gop = try next.getOrPut(self.alloc, key);
        const now_ns = nowNs();
        if (gop.found_existing) {
            self.alloc.free(gop.value_ptr.value);
            gop.value_ptr.value = try self.alloc.dupe(u8, value);
            gop.value_ptr.updated_at_ns = now_ns;
        } else {
            gop.key_ptr.* = try self.alloc.dupe(u8, key);
            gop.value_ptr.* = .{
                .value = try self.alloc.dupe(u8, value),
                .created_at_ns = now_ns,
                .updated_at_ns = now_ns,
            };
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

    fn load(self: *FileStore) !void {
        const metadata = statFileMetadata(self.path) catch |err| switch (err) {
            error.FileNotFound => {
                self.observed_metadata = null;
                self.last_reload_failed = false;
                return;
            },
            else => return err,
        };
        if (metadata == null) {
            self.observed_metadata = null;
            self.last_reload_failed = false;
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
        self.last_reload_failed = false;
    }

    fn refreshIfChangedLocked(self: *FileStore) !bool {
        const metadata = statFileMetadata(self.path) catch |err| switch (err) {
            error.FileNotFound => {
                if (self.observed_metadata != null) {
                    self.last_reload_failed = true;
                    std.log.warn("secret store file missing; keeping last known good snapshot path={s}", .{self.path});
                } else {
                    self.last_reload_failed = false;
                }
                return false;
            },
            else => return err,
        };
        if (metadata == null) {
            if (self.observed_metadata != null) {
                self.last_reload_failed = true;
                std.log.warn("secret store file missing; keeping last known good snapshot path={s}", .{self.path});
            } else {
                self.last_reload_failed = false;
            }
            return false;
        }
        if (self.observed_metadata) |observed| {
            if (observed.eql(metadata.?)) {
                self.last_reload_failed = false;
                return false;
            }
        }

        var next = loadEntriesFromFile(self.alloc, self.path) catch |err| {
            self.last_reload_failed = true;
            std.log.warn("secret store reload failed; keeping last known good snapshot path={s} err={}", .{ self.path, err });
            return false;
        };
        errdefer {
            deinitEntries(self.alloc, &next);
            next.deinit(self.alloc);
        }
        self.replaceEntriesLocked(&next);
        self.observed_metadata = metadata;
        self.generation_value +%= 1;
        self.last_reload_failed = false;
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
        self.last_reload_failed = false;
    }

    fn replaceEntriesLocked(self: *FileStore, next: *std.StringArrayHashMapUnmanaged(StoredSecret)) void {
        deinitEntries(self.alloc, &self.entries);
        self.entries.deinit(self.alloc);
        self.entries = next.*;
        next.* = .{};
    }

    fn lock(self: *FileStore) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn unlock(self: *FileStore) void {
        self.mutex.unlock();
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
        const gop = try entries.getOrPut(alloc, item.key);
        if (gop.found_existing) continue;
        gop.key_ptr.* = try alloc.dupe(u8, item.key);
        gop.value_ptr.* = .{
            .value = try alloc.dupe(u8, item.value),
            .created_at_ns = item.created_at_ns orelse 0,
            .updated_at_ns = item.updated_at_ns orelse item.created_at_ns orelse 0,
        };
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
        .status = if (has_env) .configured_both else .configured_keystore,
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

    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
    }

    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
        return err;
    };
}

fn deleteFile(path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), path);
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
    try std.testing.expectEqual(SecretStatus.configured_keystore, entry.status);
    try std.testing.expectEqualStrings("OPENAI_API_KEY", entry.env_var.?);

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

    try deleteFile(path);
    const missing_generation = store.generation();
    const after_missing = try store.getOwned(alloc, "openai.api_key");
    defer if (after_missing) |value| alloc.free(value);
    try std.testing.expectEqualStrings("stable", after_missing.?);
    try std.testing.expect(store.reloadFailed());
    try std.testing.expectEqual(missing_generation, store.generation());
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

test "environment secret discovery maps API key env vars" {
    const alloc = std.testing.allocator;
    const env_var = try envVarForKey(alloc, "anthropic.api_key");
    defer alloc.free(env_var);
    try std.testing.expectEqualStrings("ANTHROPIC_API_KEY", env_var);
    const key = secretKeyForEnvVar(alloc, "ANTHROPIC_API_KEY").?;
    defer alloc.free(key);
    try std.testing.expectEqualStrings("anthropic.api_key", key);
}
