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
const ExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const registry = @import("registry.zig");
const catalog_publication = @import("catalog_publication.zig");
pub const ResolvedModel = @import("model_selection.zig").ResolvedModel;

pub const tasks = [_][]const u8{ "embedders", "rerankers", "chunkers", "generators", "extractors", "deciders", "rewriters", "readers", "transcribers", "classifiers" };

pub fn scopes(task: []const u8) []const []const u8 {
    if (std.mem.eql(u8, task, "readers")) return &.{ "readers", "transcribers", "generators" };
    if (std.mem.eql(u8, task, "deciders")) return &.{ "extractors", "embedders" };
    return &.{}; // Other task categories use their own physical scope.
}
pub fn firstScopeWins(task: []const u8) bool {
    return std.mem.eql(u8, task, "deciders");
}

const Owner = struct { index: ?usize, priority: u2 = 0 };
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
    publications: []const ?[32]u8 = &.{},
    damaged_installations: []const usize = &.{},
    owners: [tasks.len]std.StringHashMapUnmanaged(Owner) = @splat(.empty),
    watches: []const Watch = &.{},
    generation: catalog_publication.Generation = .{},

    pub fn init(allocator: std.mem.Allocator, io: std.Io, models_dir: []const u8) !Inventory {
        return initWithControl(allocator, io, models_dir, .{});
    }

    fn initWithControl(allocator: std.mem.Allocator, io: std.Io, models_dir: []const u8, control: ExecutionControl) !Inventory {
        try control.check();
        var temporary = std.heap.ArenaAllocator.init(allocator);
        defer temporary.deinit();
        var source = registry.ModelRegistry.init(temporary.allocator(), models_dir);
        var result: Inventory = .{ .arena = .init(allocator), .models_dir = undefined, .discovered = undefined, .names = undefined };
        errdefer result.deinit();
        const a = result.arena.allocator();
        result.models_dir = try a.dupe(u8, models_dir);
        result.generation = try catalog_publication.read(a, io, models_dir);
        var paths = std.StringHashMapUnmanaged(?Stamp).empty;
        // Capture directory generations before enumeration, including empty
        // owner directories. Publishing their first model need not touch root.
        const roots = try a.alloc([]const u8, tasks.len + 1);
        roots[0] = result.models_dir;
        for (tasks, roots[1..]) |task, *root| root.* = try std.fs.path.join(a, &.{ models_dir, task });
        for (roots) |root| {
            try control.check();
            try watchPath(&paths, a, io, root);
            var dir = std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true }) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => continue,
                else => return err,
            };
            defer dir.close(io);
            var iterator = dir.iterate();
            while (try iterator.next(io)) |entry| {
                try control.check();
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
                    try control.check();
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
        const found = try source.discoverInstallationsWithControl(io, a, control);
        for (found) |entry| {
            try watchInstallation(&paths, a, io, entry.path);
        }

        const entries = try a.alloc(registry.ModelEntry, found.len);
        const names = try a.alloc([]const []const u8, found.len);
        const publications = try a.alloc(?[32]u8, found.len);
        var damaged_installations: std.ArrayList(usize) = .empty;
        for (found, entries, names, publications, 0..) |entry, *out, *selectors, *publication, index| {
            try control.check();
            const proof = entry.installation_selection;
            const managed = proof.names;
            publication.* = if (proof.rejected) null else proof.publication;
            selectors.* = if (proof.damaged) &.{} else if (managed.len != 0)
                managed
            else if (std.mem.indexOfAny(u8, entry.name, ":@") == null) names: {
                const literal = try a.alloc([]const u8, 1);
                literal[0] = try a.dupe(u8, entry.name);
                break :names literal;
            } else &.{};
            out.* = entry;
            if (proof.damaged) {
                try damaged_installations.append(a, index);
            } else out.installation_selection = .{};
            out.path = try a.dupe(u8, entry.path);
            // Qualified, immutable keys keep distinct capability descriptors.
            const canonical = if (managed.len == 4) managed[2] else if (managed.len > 0) managed[managed.len - 1] else entry.name;
            out.name = try a.dupe(u8, canonical);
        }
        result.discovered = entries;
        result.names = names;
        result.publications = publications;
        result.damaged_installations = try damaged_installations.toOwnedSlice(a);
        try result.buildOwners(control);
        const watches = try a.alloc(Watch, paths.count());
        var iterator = paths.iterator();
        var i: usize = 0;
        while (iterator.next()) |path| : (i += 1) watches[i] = .{ .path = path.key_ptr.*, .stamp = path.value_ptr.* };
        result.watches = watches;
        return result;
    }

    pub fn loadStable(allocator: std.mem.Allocator, io: std.Io, root: []const u8) !Inventory {
        return loadStableWithControl(allocator, io, root, .{});
    }

    pub fn loadStableWithControl(allocator: std.mem.Allocator, io: std.Io, root: []const u8, control: ExecutionControl) !Inventory {
        for (0..3) |_| {
            var inventory = try initWithControl(allocator, io, root, control);
            errdefer inventory.deinit();
            if (!try inventory.changed(io, control)) return inventory;
            inventory.deinit();
        }
        return error.ModelInventoryChanged;
    }

    pub fn deinit(self: *Inventory) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn buildOwners(self: *Inventory, control: ExecutionControl) !void {
        const a = self.arena.allocator();
        for (tasks, &self.owners) |task, *owners| {
            const aliases = scopes(task);
            const physical: []const []const u8 = if (aliases.len > 0) aliases else &.{task};
            for (physical) |scope| {
                var local = std.StringHashMapUnmanaged(Owner).empty;
                for (self.discovered, self.names, 0..) |entry, selectors, index| {
                    try control.check();
                    if (!registry.modelInTaskScope(self.models_dir, entry.path, scope)) continue;
                    for (selectors) |selector| {
                        const bare = std.mem.indexOfAny(u8, selector, ":@") == null;
                        const root = if (bare) try std.fs.path.join(a, &.{ self.models_dir, selector }) else "";
                        const legacy = if (bare) try std.fs.path.join(a, &.{ self.models_dir, scope, selector }) else "";
                        const priority: u2 = if (bare and std.mem.eql(u8, root, entry.path)) 2 else if (bare and std.mem.eql(u8, legacy, entry.path)) 1 else 0;
                        const owner = try local.getOrPut(a, selector);
                        if (!owner.found_existing or priority > owner.value_ptr.priority) {
                            owner.value_ptr.* = .{ .index = index, .priority = priority };
                        } else if (priority == 0 and (owner.value_ptr.index == null or !std.mem.eql(u8, self.discovered[owner.value_ptr.index.?].path, entry.path))) owner.value_ptr.index = null;
                    }
                }
                var iterator = local.iterator();
                while (iterator.next()) |candidate| {
                    const owner = try owners.getOrPut(a, candidate.key_ptr.*);
                    if (owner.found_existing and firstScopeWins(task)) continue;
                    if (!owner.found_existing) owner.value_ptr.* = candidate.value_ptr.* else if (owner.value_ptr.index == null or candidate.value_ptr.index == null or !std.mem.eql(u8, self.discovered[owner.value_ptr.index.?].path, self.discovered[candidate.value_ptr.index.?].path)) owner.value_ptr.index = null;
                }
            }
        }
    }

    pub fn lookupIndex(self: *const Inventory, selector: []const u8, task: []const u8) !?usize {
        for (tasks, self.owners) |name, owners| if (std.mem.eql(u8, name, task)) {
            const found = owners.get(selector) orelse return null;
            return found.index orelse error.AmbiguousModelIdentifier;
        };
        return null;
    }

    /// A valid owner always takes precedence over a damaged declaration.
    /// Scoped replica probes can revalidate a damaged installation directly.
    pub fn lookupDamagedIndex(self: *const Inventory, selector: []const u8, task: []const u8) !?usize {
        var result: ?usize = null;
        for (self.damaged_installations) |index| {
            const entry = self.discovered[index];
            const candidate = entry.installation_selection;
            if (!registry.modelInTaskScope(self.models_dir, entry.path, task)) continue;
            const matched = if (candidate.names.len == 0)
                std.mem.indexOfAny(u8, selector, ":@") == null and std.mem.eql(u8, entry.name, selector)
            else for (candidate.names) |name| {
                if (std.mem.eql(u8, name, selector)) break true;
            } else false;
            if (!matched) continue;
            if (result != null) return error.InvalidManagedDownload;
            result = index;
        }
        return result;
    }

    pub fn lookup(self: *const Inventory, selector: []const u8, task: ?[]const u8) !?[]const u8 {
        if (task) |scope| {
            const index = (try self.lookupIndex(selector, scope)) orelse return null;
            return self.discovered[index].path;
        }
        var selected: ?[]const u8 = null;
        for (self.discovered, self.names) |entry, selectors| for (selectors) |name| {
            if (!std.mem.eql(u8, name, selector)) continue;
            if (selected != null and !std.mem.eql(u8, selected.?, entry.path)) return error.AmbiguousModelIdentifier;
            selected = entry.path;
        };
        return selected;
    }

    fn changed(self: *const Inventory, io: std.Io, control: ExecutionControl) !bool {
        const current = try catalog_publication.read(self.arena.child_allocator, io, self.models_dir);
        if (!std.meta.eql(self.generation, current)) return true;
        if (current.kind != .legacy) return false;
        for (self.watches) |watch| {
            try control.check();
            if (!std.meta.eql(watch.stamp, try stamp(io, watch.path))) return true;
        }
        return false;
    }

    pub fn requireCurrentGeneration(self: *const Inventory, io: std.Io) !void {
        return self.requireCurrentGenerationWithControl(io, .{});
    }

    pub fn requireCurrentGenerationWithControl(self: *const Inventory, io: std.Io, control: ExecutionControl) !void {
        if (try self.changed(io, control)) return error.ModelArtifactsChanging;
    }

    fn clone(self: *const Inventory, allocator: std.mem.Allocator) !Inventory {
        var copy: Inventory = .{ .arena = .init(allocator), .models_dir = undefined, .discovered = undefined, .names = undefined };
        errdefer copy.deinit();
        const a = copy.arena.allocator();
        copy.models_dir = try a.dupe(u8, self.models_dir);
        copy.generation = self.generation;
        const entries = try a.alloc(registry.ModelEntry, self.discovered.len);
        const names = try a.alloc([]const []const u8, self.names.len);
        for (self.discovered, self.names, entries, names) |entry, selectors, *out, *owned| {
            out.* = entry;
            out.name = try a.dupe(u8, entry.name);
            out.path = try a.dupe(u8, entry.path);
            if (entry.installation_selection.damaged) {
                const damaged_names = try a.alloc([]const u8, entry.installation_selection.names.len);
                for (entry.installation_selection.names, damaged_names) |name, *value| value.* = try a.dupe(u8, name);
                out.installation_selection.names = damaged_names;
            }
            const strings = try a.alloc([]const u8, selectors.len);
            for (selectors, strings) |name, *value| value.* = try a.dupe(u8, name);
            owned.* = strings;
        }
        copy.discovered = entries;
        copy.names = names;
        copy.publications = try a.dupe(?[32]u8, self.publications);
        copy.damaged_installations = try a.dupe(usize, self.damaged_installations);
        // Full catalogs retain legacy watch evidence until serialization ends.
        const watches = try a.alloc(Watch, self.watches.len);
        for (self.watches, watches) |watch, *owned| owned.* = .{ .path = try a.dupe(u8, watch.path), .stamp = watch.stamp };
        copy.watches = watches;
        try copy.buildOwners(.{});
        return copy;
    }
};

pub const Cache = struct {
    allocator: std.mem.Allocator,
    // Only ownership and publication use this lock; filesystem IO never does.
    mutex: std.Io.Mutex = .init,
    refresh_mutex: std.Io.Mutex = .init,
    current: ?*Slot = null,
    epoch: usize = 0,
    rebuilds: usize = 0,

    const Slot = struct {
        inventory: Inventory,
        refs: usize = 1,
        epoch: usize,
        rebuilt_at: std.Io.Timestamp,
    };
    pub const Lease = struct {
        cache: *Cache,
        slot: *Slot,
        io: std.Io,
        pub fn inventory(self: Lease) *const Inventory {
            return &self.slot.inventory;
        }
        pub fn invalidateIfCurrent(self: Lease) void {
            self.cache.mutex.lockUncancelable(self.io);
            defer self.cache.mutex.unlock(self.io);
            if (self.cache.current == self.slot and self.cache.epoch == self.slot.epoch) self.cache.epoch +%= 1;
        }

        pub fn release(self: Lease) void {
            self.cache.mutex.lockUncancelable(self.io);
            self.slot.refs -= 1;
            const destroy = self.slot.refs == 0;
            self.cache.mutex.unlock(self.io);
            if (destroy) {
                self.slot.inventory.deinit();
                self.cache.allocator.destroy(self.slot);
            }
        }
    };

    pub fn init(allocator: std.mem.Allocator) Cache {
        return .{ .allocator = allocator };
    }
    pub fn deinit(self: *Cache) void {
        // The owner joins request workers before destroying the node.
        if (self.current) |slot| {
            std.debug.assert(slot.refs == 1);
            slot.inventory.deinit();
            self.allocator.destroy(slot);
        }
    }
    fn retain(self: *Cache, io: std.Io) !?Lease {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        const slot = self.current orelse return null;
        if (slot.epoch != self.epoch) return null;
        slot.refs += 1;
        return .{ .cache = self, .slot = slot, .io = io };
    }
    fn acquire(self: *Cache, io: std.Io, root: []const u8, catalog_backstop: bool, control: ExecutionControl) !Lease {
        try control.check();
        if (try self.retain(io)) |lease| {
            errdefer lease.release();
            const unchanged = std.mem.eql(u8, root, lease.slot.inventory.models_dir) and !try lease.slot.inventory.changed(io, control);
            if (unchanged) {
                const age = std.Io.Timestamp.now(io, .awake).nanoseconds - lease.slot.rebuilt_at.nanoseconds;
                if ((!catalog_backstop and lease.slot.inventory.generation.kind != .legacy) or age < 5 * std.time.ns_per_s) return lease;
                // Another caller owns the bounded backstop refresh. Managed
                // publications are unchanged and execution revalidates its
                // selected receipt, so readers can retain this generation.
                if (!self.refresh_mutex.tryLock()) return lease;
            } else try self.lockRefresh(io, control);
            lease.release();
        } else try self.lockRefresh(io, control);
        defer self.refresh_mutex.unlock(io);

        // Coalesce queued discovery/lookup refreshes after the first publishes.
        if (try self.retain(io)) |lease| {
            errdefer lease.release();
            const age = std.Io.Timestamp.now(io, .awake).nanoseconds - lease.slot.rebuilt_at.nanoseconds;
            if (((!catalog_backstop and lease.slot.inventory.generation.kind != .legacy) or age < 5 * std.time.ns_per_s) and std.mem.eql(u8, root, lease.slot.inventory.models_dir) and !try lease.slot.inventory.changed(io, control)) return lease;
            lease.release();
        }
        self.mutex.lockUncancelable(io);
        const epoch = self.epoch;
        self.mutex.unlock(io);
        var next = try Inventory.loadStableWithControl(self.allocator, io, root, control);
        errdefer next.deinit();
        const slot = try self.allocator.create(Slot);
        errdefer self.allocator.destroy(slot);
        slot.* = .{ .inventory = next, .epoch = epoch, .rebuilt_at = std.Io.Timestamp.now(io, .awake), .refs = 2 };
        self.mutex.lockUncancelable(io);
        if (self.epoch != epoch) {
            self.mutex.unlock(io);
            return error.ModelInventoryChanged;
        }
        const previous = self.current;
        self.current = slot;
        self.rebuilds += 1;
        self.mutex.unlock(io);
        if (previous) |old| (Lease{ .cache = self, .slot = old, .io = io }).release();
        return .{ .cache = self, .slot = slot, .io = io };
    }

    fn lockRefresh(self: *Cache, io: std.Io, control: ExecutionControl) !void {
        if (control.deadline_ns == null and control.cancellation == null and control.check_fn == null)
            return self.refresh_mutex.lock(io);
        while (!self.refresh_mutex.tryLock()) {
            try control.check();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    pub fn borrowInventory(self: *Cache, io: std.Io, root: []const u8, control: ExecutionControl) !Lease {
        return self.acquire(io, root, true, control);
    }

    pub fn invalidate(self: *Cache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.epoch +%= 1;
    }

    pub fn borrowForSelector(self: *Cache, io: std.Io, root: []const u8, selector: []const u8, task: []const u8, control: ExecutionControl) !Lease {
        const lease = try self.acquire(io, root, false, control);
        errdefer lease.release();
        // Preserve the bounded raw-filesystem import backstop on misses only.
        if ((lease.inventory().lookupIndex(selector, task) catch return lease) != null) return lease;
        const age = std.Io.Timestamp.now(io, .awake).nanoseconds - lease.slot.rebuilt_at.nanoseconds;
        if (age < 5 * std.time.ns_per_s) return lease;
        const refreshed = try self.acquire(io, root, true, control);
        lease.release();
        return refreshed;
    }

    pub fn snapshot(self: *Cache, allocator: std.mem.Allocator, io: std.Io, root: []const u8) !Inventory {
        const lease = try self.acquire(io, root, true, .{});
        defer lease.release();
        return lease.slot.inventory.clone(allocator);
    }

    // Path-only adapter for metadata inspection and existing registry tooling.
    pub fn resolve(self: *Cache, allocator: std.mem.Allocator, io: std.Io, root: []const u8, requested: []const u8, task: ?[]const u8) !?[]u8 {
        return if (try self.resolveSelection(allocator, io, root, requested, task)) |selected| @constCast(selected.path) else null;
    }
    pub fn resolveSelection(self: *Cache, allocator: std.mem.Allocator, io: std.Io, root: []const u8, requested: []const u8, task: ?[]const u8) !?ResolvedModel {
        return self.resolveSelectionWithResident(allocator, io, root, requested, task, null);
    }

    pub const ResidentPublication = struct {
        context: *anyopaque,
        matches: ?*const fn (*anyopaque, std.Io, []const u8, [32]u8) anyerror!bool = null,
        prepare: ?*const fn (*anyopaque, std.Io, []const u8, []const u8, ?[32]u8) anyerror![32]u8 = null,
        control: ExecutionControl = .{},
    };

    pub fn resolveSelectionWithResident(self: *Cache, allocator: std.mem.Allocator, io: std.Io, root: []const u8, requested: []const u8, task: ?[]const u8, resident: ?ResidentPublication) !?ResolvedModel {
        // Full catalogs and misses get a bounded backstop for raw filesystem
        // imports. Existing coordinated pinned hits never scan other models.
        const control: ExecutionControl = if (resident) |candidate| candidate.control else .{};
        var retried_catalog = false;
        const lookup = blk: while (true) {
            const lease = try self.acquire(io, root, retried_catalog, control);
            defer lease.release();
            const inventory = lease.inventory();
            const index = if (task) |scope| try inventory.lookupIndex(requested, scope) else null;
            const path = if (index) |i| inventory.discovered[i].path else if (task == null) try inventory.lookup(requested, null) else null;
            const selected = path orelse {
                if (!std.meta.eql(lease.slot.inventory.generation, try catalog_publication.read(allocator, io, root))) return error.ModelArtifactsChanging;
                const age = std.Io.Timestamp.now(io, .awake).nanoseconds - lease.slot.rebuilt_at.nanoseconds;
                if (!retried_catalog and age >= 5 * std.time.ns_per_s) {
                    retried_catalog = true;
                    continue;
                }
                return null;
            };
            break :blk .{
                .path = try allocator.dupe(u8, selected),
                .generation = inventory.generation,
                .publication = if (index) |i| inventory.publications[i] else null,
            };
        };
        const path = lookup.path;
        errdefer allocator.free(path);
        if (resident) |candidate| {
            if (candidate.prepare) |prepare| {
                const publication = prepare(candidate.context, io, path, requested, lookup.publication) catch |err| switch (err) {
                    error.ModelNotFound => {
                        allocator.free(path);
                        self.invalidate(io);
                        return null;
                    },
                    else => return err,
                };
                try catalog_publication.require(allocator, io, root, lookup.generation);
                return .{ .path = path, .publication = publication };
            }
            if (candidate.matches) |matches| if (lookup.publication) |publication| {
                if (try matches(candidate.context, io, path, publication)) {
                    try catalog_publication.require(allocator, io, root, lookup.generation);
                    return .{ .path = path, .publication = publication };
                }
            };
        }
        var temporary = std.heap.ArenaAllocator.init(allocator);
        defer temporary.deinit();
        const proof = try registry.managedModelSelectionAlloc(temporary.allocator(), io, path);
        if (!std.meta.eql(lookup.generation, try catalog_publication.read(allocator, io, root))) return error.ModelArtifactsChanging;
        if (!proof.rejected) {
            for (proof.names) |name| if (std.mem.eql(u8, name, requested)) return .{ .path = path, .publication = proof.publication };
            if (proof.names.len == 0 and std.mem.indexOfAny(u8, requested, ":@") == null) return .{ .path = path, .publication = proof.publication };
        }
        allocator.free(path);
        self.invalidate(io);
        return null;
    }
};
