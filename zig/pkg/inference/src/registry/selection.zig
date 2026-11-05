// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Receipt-backed selector inventory shared by discovery and execution.
//! Publication metadata invalidates the index; only the selected receipt's
//! artifacts are revalidated on the execution path. No model weights are read.
const std = @import("std");
const registry = @import("registry.zig");

pub const tasks = [_][]const u8{ "embedders", "rerankers", "chunkers", "generators", "extractors", "deciders", "rewriters", "readers", "transcribers", "classifiers" };

pub fn scopes(task: []const u8) []const []const u8 {
    if (std.mem.eql(u8, task, "readers")) return &.{ "readers", "transcribers", "generators" };
    if (std.mem.eql(u8, task, "deciders")) return &.{"extractors"};
    return &.{}; // Other task categories use their own physical scope.
}

const Owner = struct { path: ?[]const u8, priority: u2 = 0 };
const Stamp = struct { inode: u64, size: u64, modified: i128 };
const Watch = struct { path: []const u8, stamp: ?Stamp };

fn stamp(io: std.Io, path: []const u8) !?Stamp {
    const stat = std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return null,
        else => return err,
    };
    return .{ .inode = stat.inode, .size = stat.size, .modified = stat.mtime.toNanoseconds() };
}

fn directoryExists(io: std.Io, path: []const u8) !bool {
    const info = std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    return info.kind == .directory;
}

fn watchPath(paths: *std.StringHashMapUnmanaged(?Stamp), allocator: std.mem.Allocator, io: std.Io, path: []const u8) !void {
    if (paths.contains(path)) return;
    try paths.put(allocator, try allocator.dupe(u8, path), try stamp(io, path));
}

fn watchInstallation(paths: *std.StringHashMapUnmanaged(?Stamp), allocator: std.mem.Allocator, io: std.Io, path: []const u8) !void {
    try watchPath(paths, allocator, io, path);
    for ([_][]const u8{ ".antfly-download-complete.json", ".antfly-download-plan.json", ".antfly-download-in-progress" }) |name| {
        try watchPath(paths, allocator, io, try std.fs.path.join(allocator, &.{ path, name }));
    }
}

pub const Inventory = struct {
    arena: std.heap.ArenaAllocator,
    models_dir: []const u8,
    discovered: []const registry.ModelEntry,
    names: []const []const []const u8,
    owners: [tasks.len]std.StringHashMapUnmanaged(Owner) = @splat(.empty),
    watches: []const Watch = &.{},

    pub fn init(allocator: std.mem.Allocator, io: std.Io, models_dir: []const u8) !Inventory {
        var temporary = std.heap.ArenaAllocator.init(allocator);
        defer temporary.deinit();
        var source = registry.ModelRegistry.init(temporary.allocator(), models_dir);
        var result: Inventory = .{ .arena = .init(allocator), .models_dir = undefined, .discovered = undefined, .names = undefined };
        errdefer result.deinit();
        const a = result.arena.allocator();
        result.models_dir = try a.dupe(u8, models_dir);
        var paths = std.StringHashMapUnmanaged(?Stamp).empty;
        // Capture directory generations before enumeration, including empty
        // owner directories. Publishing their first model need not touch root.
        const roots = try a.alloc([]const u8, tasks.len + 1);
        roots[0] = result.models_dir;
        for (tasks, roots[1..]) |task, *root| root.* = try std.fs.path.join(a, &.{ models_dir, task });
        for (roots) |root| {
            try watchPath(&paths, a, io, root);
            var dir = std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => continue,
                else => return err,
            };
            defer dir.close(io);
            var iterator = dir.iterate();
            while (try iterator.next(io)) |entry| {
                if (entry.name.len == 0 or entry.name[0] == '.') continue;
                if (entry.kind != .directory and entry.kind != .sym_link and entry.kind != .unknown) continue;
                const child = try std.fs.path.join(a, &.{ root, entry.name });
                if (entry.kind != .directory and !try directoryExists(io, child)) continue;
                try watchInstallation(&paths, a, io, child);
                var owner = std.Io.Dir.cwd().openDir(io, child, .{ .iterate = true }) catch |err| switch (err) {
                    error.FileNotFound, error.NotDir => continue,
                    else => return err,
                };
                defer owner.close(io);
                var candidates = owner.iterate();
                while (try candidates.next(io)) |candidate| {
                    if (candidate.name.len == 0 or candidate.name[0] == '.') continue;
                    if (candidate.kind != .directory and candidate.kind != .sym_link and candidate.kind != .unknown) continue;
                    // Incomplete managed directories are absent from discovery,
                    // but completing their publication must invalidate the index.
                    const path = try std.fs.path.join(a, &.{ child, candidate.name });
                    if (candidate.kind != .directory and !try directoryExists(io, path)) continue;
                    try watchInstallation(&paths, a, io, path);
                }
            }
        }
        const found = try source.discoverInstallations(io);
        for (found) |entry| {
            try watchInstallation(&paths, a, io, entry.path);
        }

        const entries = try a.alloc(registry.ModelEntry, found.len);
        const names = try a.alloc([]const []const u8, found.len);
        for (found, entries, names) |entry, *out, *selectors| {
            var candidate = std.heap.ArenaAllocator.init(allocator);
            defer candidate.deinit();
            const managed = try registry.managedModelSelectorsAlloc(candidate.allocator(), io, entry.path);
            selectors.* = try a.alloc([]const u8, if (managed.len > 0) managed.len else if (std.mem.indexOfAny(u8, entry.name, ":@") == null) @as(usize, 1) else 0);
            if (managed.len > 0) {
                for (managed, @constCast(selectors.*)) |name, *owned| owned.* = try a.dupe(u8, name);
            } else if (selectors.len > 0) @constCast(selectors.*)[0] = try a.dupe(u8, entry.name);
            out.* = entry;
            out.path = try a.dupe(u8, entry.path);
            // Qualified, immutable keys keep distinct capability descriptors.
            const canonical = if (managed.len == 4) managed[2] else if (managed.len > 0) managed[managed.len - 1] else entry.name;
            out.name = try a.dupe(u8, canonical);
        }
        result.discovered = entries;
        result.names = names;
        try result.buildOwners();
        const watches = try a.alloc(Watch, paths.count());
        var iterator = paths.iterator();
        var i: usize = 0;
        while (iterator.next()) |path| : (i += 1) watches[i] = .{ .path = path.key_ptr.*, .stamp = path.value_ptr.* };
        result.watches = watches;
        return result;
    }

    pub fn loadStable(allocator: std.mem.Allocator, io: std.Io, root: []const u8) !Inventory {
        for (0..3) |_| {
            var inventory = try init(allocator, io, root);
            errdefer inventory.deinit();
            if (!try inventory.changed(io)) return inventory;
            inventory.deinit();
        }
        return error.ModelInventoryChanged;
    }

    pub fn deinit(self: *Inventory) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn buildOwners(self: *Inventory) !void {
        const a = self.arena.allocator();
        for (tasks, &self.owners) |task, *owners| {
            const aliases = scopes(task);
            const physical: []const []const u8 = if (aliases.len > 0) aliases else &.{task};
            for (physical) |scope| {
                var local = std.StringHashMapUnmanaged(Owner).empty;
                for (self.discovered, self.names) |entry, selectors| {
                    if (!registry.modelInTaskScope(self.models_dir, entry.path, scope)) continue;
                    for (selectors) |selector| {
                        const bare = std.mem.indexOfAny(u8, selector, ":@") == null;
                        const root = if (bare) try std.fs.path.join(a, &.{ self.models_dir, selector }) else "";
                        const legacy = if (bare) try std.fs.path.join(a, &.{ self.models_dir, scope, selector }) else "";
                        const priority: u2 = if (bare and std.mem.eql(u8, root, entry.path)) 2 else if (bare and std.mem.eql(u8, legacy, entry.path)) 1 else 0;
                        const owner = try local.getOrPut(a, selector);
                        if (!owner.found_existing or priority > owner.value_ptr.priority) {
                            owner.value_ptr.* = .{ .path = entry.path, .priority = priority };
                        } else if (priority == 0 and (owner.value_ptr.path == null or !std.mem.eql(u8, owner.value_ptr.path.?, entry.path))) owner.value_ptr.path = null;
                    }
                }
                var iterator = local.iterator();
                while (iterator.next()) |candidate| {
                    const owner = try owners.getOrPut(a, candidate.key_ptr.*);
                    if (!owner.found_existing) owner.value_ptr.* = candidate.value_ptr.* else if (owner.value_ptr.path == null or candidate.value_ptr.path == null or !std.mem.eql(u8, owner.value_ptr.path.?, candidate.value_ptr.path.?)) owner.value_ptr.path = null;
                }
            }
        }
    }

    pub fn lookup(self: *const Inventory, selector: []const u8, task: ?[]const u8) !?[]const u8 {
        if (task) |scope| {
            for (tasks, self.owners) |name, owners| if (std.mem.eql(u8, name, scope)) {
                const found = owners.get(selector) orelse return null;
                return found.path orelse error.AmbiguousModelIdentifier;
            };
            return null;
        }
        var selected: ?[]const u8 = null;
        for (self.discovered, self.names) |entry, selectors| for (selectors) |name| {
            if (!std.mem.eql(u8, name, selector)) continue;
            if (selected != null and !std.mem.eql(u8, selected.?, entry.path)) return error.AmbiguousModelIdentifier;
            selected = entry.path;
        };
        return selected;
    }

    fn changed(self: *const Inventory, io: std.Io) !bool {
        for (self.watches) |watch| if (!std.meta.eql(watch.stamp, try stamp(io, watch.path))) return true;
        return false;
    }

    fn clone(self: *const Inventory, allocator: std.mem.Allocator) !Inventory {
        var copy: Inventory = .{ .arena = .init(allocator), .models_dir = undefined, .discovered = undefined, .names = undefined };
        errdefer copy.deinit();
        const a = copy.arena.allocator();
        copy.models_dir = try a.dupe(u8, self.models_dir);
        const entries = try a.alloc(registry.ModelEntry, self.discovered.len);
        const names = try a.alloc([]const []const u8, self.names.len);
        for (self.discovered, self.names, entries, names) |entry, selectors, *out, *owned| {
            out.* = entry;
            out.name = try a.dupe(u8, entry.name);
            out.path = try a.dupe(u8, entry.path);
            const strings = try a.alloc([]const u8, selectors.len);
            for (selectors, strings) |name, *value| value.* = try a.dupe(u8, name);
            owned.* = strings;
        }
        copy.discovered = entries;
        copy.names = names;
        try copy.buildOwners();
        return copy;
    }
};

pub const Cache = struct {
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    inventory: ?Inventory = null,
    rebuilt_at: ?std.Io.Timestamp = null,
    rebuilds: usize = 0,

    pub fn init(allocator: std.mem.Allocator) Cache {
        return .{ .allocator = allocator };
    }
    pub fn deinit(self: *Cache) void {
        if (self.inventory) |*inventory| inventory.deinit();
    }

    fn refresh(self: *Cache, io: std.Io, root: []const u8, force: bool) !void {
        const now = std.Io.Timestamp.now(io, .awake);
        if (!force) if (self.inventory) |*inventory| {
            if (std.mem.eql(u8, root, inventory.models_dir)) {
                // A bounded full refresh also detects out-of-band edits to
                // nested artifacts, outside the managed publication protocol.
                if (self.rebuilt_at) |rebuilt| if (now.nanoseconds - rebuilt.nanoseconds < 5 * std.time.ns_per_s and !try inventory.changed(io)) {
                    return;
                };
            }
        };
        const next = try Inventory.loadStable(self.allocator, io, root);
        if (self.inventory) |*old| old.deinit();
        self.inventory = next;
        self.rebuilt_at = now;
        self.rebuilds += 1;
    }

    /// Explicit publication/refresh notification for in-process installers.
    pub fn invalidate(self: *Cache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.rebuilt_at = null;
    }

    pub fn snapshot(self: *Cache, allocator: std.mem.Allocator, io: std.Io, root: []const u8) !Inventory {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        try self.refresh(io, root, true);
        return self.inventory.?.clone(allocator);
    }

    pub fn resolve(self: *Cache, allocator: std.mem.Allocator, io: std.Io, root: []const u8, requested: []const u8, task: ?[]const u8) !?[]u8 {
        const path = blk: {
            try self.mutex.lock(io);
            defer self.mutex.unlock(io);
            try self.refresh(io, root, false);
            const selected = (try self.inventory.?.lookup(requested, task)) orelse return null;
            break :blk try allocator.dupe(u8, selected);
        };
        errdefer allocator.free(path);
        // Never let a cached pin authorize replaced/incomplete artifacts.
        var temporary = std.heap.ArenaAllocator.init(allocator);
        defer temporary.deinit();
        const names = try registry.managedModelSelectorsAlloc(temporary.allocator(), io, path);
        for (names) |name| if (std.mem.eql(u8, name, requested)) return path;
        if (names.len == 0 and std.mem.indexOfAny(u8, requested, ":@") == null) return path;
        allocator.free(path);
        self.invalidate(io);
        return null;
    }
};
