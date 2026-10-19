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

//! Kernel coverage of discovery/configuration dependencies. This is opt-in:
//! kernel-invisible writers still require force_rescan. Incomplete coverage,
//! allocation loss or overflow uses ordinary discovery and source reads.
const std = @import("std");
const Watch = @import("source_watch.zig").Watch;
const Token = @import("source_watch.zig").Token;
pub const Journal = struct {
    io: std.Io,
    budget: @import("antfly_regex").Budget,
    watch: Watch,
    native: @import("path_watch.zig").Events,
    displays: std.StringHashMapUnmanaged([]const u8) = .empty,
    generation: ?*const @import("workspace_store.zig").Generation = null,
    directory_aliases: bool = false,
    allow_native_sources: bool = false,
    dependencies: std.StringHashMapUnmanaged(bool) = .empty,
    inventories: std.StringHashMapUnmanaged(?@import("directory_stamp.zig").Stamp) = .empty,
    native_active: bool = false,
    native_refresh: bool = false,
    native_covered: bool = false,
    native_epoch: u64 = 0,
    rejection: []const u8 = "none",
    approved_sequences: [4096]u64 = @splat(std.math.maxInt(u64)),
    approved_generations: [4096]u64 = @splat(0),
    tokens: std.StringHashMapUnmanaged(Token) = .empty,
    // Borrowed paths route ancestor invalidations to dependent descendants.
    routes: [4096]std.ArrayList([]const u8) = @splat(.empty),
    dependency_routes: [4096]std.ArrayList([]const u8) = @splat(.empty),
    roots: std.ArrayList(Root) = .empty,
    membership_dirs: std.ArrayList(Membership) = .empty,
    allow_membership: bool = false,
    path_checks: usize = 0,
    directory_checks: usize = 0,
    coverage: bool = true,
    sources_covered: bool = false,
    source_epoch: u64 = 0,
    dependency_epoch: u64 = 0,
    sealed: bool = false,
    pub const Root = struct { path: []const u8, display: []const u8 };
    pub const Membership = struct { path: []const u8, display: []const u8, stamp: @import("directory_stamp.zig").Stamp, native_names: bool = false };
    pub fn init(a: std.mem.Allocator, io: std.Io) Journal {
        return .{ .io = io, .budget = .{ .backing = a, .limit = 1024 * 1024 }, .watch = Watch.init(), .native = .init(a, io) };
    }
    pub fn deinit(self: *Journal) void {
        self.watch.deinit();
        self.native.deinit();
        self.clearMetadata();
    }
    fn clearMetadata(self: *Journal) void {
        for (self.roots.items) |root| {
            self.budget.allocator().free(root.path);
            self.budget.allocator().free(root.display);
        }
        self.roots.deinit(self.budget.allocator());
        self.roots = .empty;
        self.membership_dirs.deinit(self.budget.allocator());
        self.membership_dirs = .empty;
        for (&self.routes) |*route| route.deinit(self.budget.allocator());
        self.routes = @splat(.empty);
        for (&self.dependency_routes) |*route| route.deinit(self.budget.allocator());
        self.dependency_routes = @splat(.empty);
        self.freeDisplays();
        var inventories = self.inventories.keyIterator();
        while (inventories.next()) |key| self.budget.allocator().free(key.*);
        self.inventories.deinit(self.budget.allocator());
        self.inventories = .empty;
        var dependencies = self.dependencies.keyIterator();
        while (dependencies.next()) |key| self.budget.allocator().free(key.*);
        self.dependencies.deinit(self.budget.allocator());
        self.dependencies = .empty;
        var it = self.tokens.keyIterator();
        while (it.next()) |key| self.budget.allocator().free(key.*);
        self.tokens.deinit(self.budget.allocator());
        self.tokens = .empty;
    }
    pub fn reset(self: *Journal) void {
        self.deinit();
        self.watch.reset();
        self.native = .init(self.budget.backing, self.io);
        self.native_active = false;
        self.directory_aliases = false;
        self.generation = null;
        self.native_covered = false;
        self.coverage = true;
        self.sources_covered = false;
        self.sealed = false;
    }
    pub fn rediscover(self: *Journal) void {
        self.native.poll();
        if (@import("builtin").os.tag == .linux and self.native.isFailed()) {
            self.reset();
            return;
        }
        // Keep healthy OS subscriptions across file-only/configuration changes.
        // Namespace/configuration witnesses are rebuilt before discovery reads;
        // no earlier membership certificate survives this transition.
        self.watch.reset();
        self.clearMetadata();
        self.native.retire(self.native.checkpoint());
        self.native_active = false;
        self.native_refresh = false;
        self.directory_aliases = false;
        self.native_covered = false;
        self.coverage = true;
        self.sources_covered = false;
        self.sealed = false;
    }
    pub fn clean(self: *Journal) bool {
        self.watch.poll();
        const intact = self.sealed and self.coverage and !self.watch.failed and self.watch.fd >= 0 and self.dependency_epoch == self.watch.epoch;
        self.native_refresh = self.native_active or !intact;
        if (!self.native_refresh and intact) return true;
        return self.namespaceClean();
    }
    fn failMetadata(self: *Journal) void {
        self.coverage = false;
        self.native.fail();
    }
    pub fn inventory(self: *Journal, path: []const u8, value: ?@import("directory_stamp.zig").Stamp) void {
        const a = self.budget.allocator();
        if (self.inventories.getPtr(path)) |old| {
            old.* = value;
            return;
        }
        const key = a.dupe(u8, path) catch {
            self.failMetadata();
            return;
        };
        self.inventories.put(a, key, value) catch {
            a.free(key);
            self.failMetadata();
        };
    }
    pub fn rearmDirectories(self: *Journal) void {
        const count = self.watch.event_count;
        for (self.watch.events.items[0..count]) |slot| for (self.routes[slot].items) |path| {
            const prior = self.tokens.getPtr(path) orelse continue;
            if (self.watch.intact(prior.*)) continue;
            var dir = std.Io.Dir.cwd().openDir(self.io, path, .{}) catch continue;
            defer dir.close(self.io);
            const token = self.watch.armDirectory(dir) orelse continue;
            if (token.generation != prior.generation or token.generation != self.approved_generations[token.slot] or token.sequence != self.approved_sequences[token.slot]) {
                self.watch.release(token);
                continue;
            }
            self.watch.certify(token);
            self.watch.release(prior.*);
            prior.* = token;
        };
        self.watch.poll();
        for (self.watch.events.items[0..self.watch.event_count]) |slot| for (self.routes[slot].items) |path| {
            if (!self.watch.intact(self.tokens.get(path).?)) return;
        };
        self.dependency_epoch = self.watch.epoch;
        self.watch.clearEvents();
    }
    pub fn registerRoot(self: *Journal, path: []const u8, display: []const u8) void {
        for (self.roots.items) |existing| if (std.mem.eql(u8, existing.path, path) and std.mem.eql(u8, existing.display, display)) return;
        const a = self.budget.allocator();
        const key = a.dupe(u8, path) catch {
            self.failMetadata();
            return;
        };
        const shown = a.dupe(u8, display) catch {
            a.free(key);
            self.failMetadata();
            return;
        };
        self.roots.append(a, .{ .path = key, .display = shown }) catch {
            a.free(key);
            a.free(shown);
            self.failMetadata();
        };
    }
    pub fn displayDirectory(self: *Journal, path: []const u8, display: []const u8) void {
        self.directory(path);
        const a = self.budget.allocator();
        const canonical = std.Io.Dir.cwd().realPathFileAlloc(self.io, path, a) catch {
            self.failMetadata();
            return;
        };
        defer a.free(canonical);
        if (self.displays.get(canonical)) |prior| {
            if (!std.mem.eql(u8, prior, display)) self.directory_aliases = true;
            return;
        }
        const key = a.dupe(u8, canonical) catch {
            self.failMetadata();
            return;
        };
        const value = a.dupe(u8, display) catch {
            a.free(key);
            self.failMetadata();
            return;
        };
        self.displays.put(a, key, value) catch {
            a.free(key);
            a.free(value);
            self.failMetadata();
        };
    }
    fn freeDisplays(self: *Journal) void {
        var it = self.displays.iterator();
        while (it.next()) |item| {
            self.budget.allocator().free(item.key_ptr.*);
            self.budget.allocator().free(item.value_ptr.*);
        }
        self.displays.deinit(self.budget.allocator());
        self.displays = .empty;
        self.generation = null;
    }
    pub fn sourceIndex(self: *Journal, path: []const u8) ?usize {
        const generation = self.generation orelse return null;
        const prefix = self.displays.get(std.fs.path.dirname(path) orelse "/") orelse return null;
        var buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
        const display = if (prefix.len == 0) std.fs.path.basename(path) else std.fmt.bufPrint(&buffer, "{s}/{s}", .{ prefix, std.fs.path.basename(path) }) catch return null;
        const entry = generation.find(display) orelse return null;
        return (@intFromPtr(entry) - @intFromPtr(generation.entries.items.ptr)) / @sizeOf(@TypeOf(entry.*));
    }
    fn rememberDependency(self: *Journal, path: []const u8, known: ?bool) void {
        const a = self.budget.allocator();
        const cwd = std.Io.Dir.cwd().realPathFileAlloc(self.io, ".", a) catch {
            self.failMetadata();
            return;
        };
        defer a.free(cwd);
        const absolute = std.fs.path.resolve(a, &.{ cwd, path }) catch {
            self.failMetadata();
            return;
        };
        if (self.dependencies.getPtr(absolute)) |existing| {
            if (known) |present| existing.* = present;
            a.free(absolute);
            return;
        }
        const present = known orelse if (std.Io.Dir.cwd().statFile(self.io, absolute, .{})) |_| true else |err| blk: {
            if (err != error.FileNotFound and err != error.NotDir) self.failMetadata();
            break :blk false;
        };
        self.dependencies.put(a, absolute, present) catch {
            a.free(absolute);
            self.failMetadata();
            return;
        };
        var parent = std.fs.path.dirname(absolute);
        while (parent) |path_name| {
            var dir = std.Io.Dir.cwd().openDir(self.io, path_name, .{}) catch {
                const next = std.fs.path.dirname(path_name);
                if (next != null and std.mem.eql(u8, next.?, path_name)) break;
                parent = next;
                continue;
            };
            dir.close(self.io);
            self.directoryWitness(path_name);
            if (self.tokens.get(path_name)) |token| {
                self.dependency_routes[token.slot].append(a, absolute) catch self.failMetadata();
            } else self.failMetadata();
            return;
        }
        self.failMetadata();
    }
    fn reject(self: *Journal, reason: []const u8) bool {
        self.rejection = reason;
        return false;
    }
    pub fn namespaceClean(self: *Journal) bool {
        self.rejection = "none";
        self.native.poll();
        self.native.lock();
        defer self.native.unlock();
        if (!self.sealed or !self.coverage or self.watch.failed or self.watch.fd < 0) return self.reject("incomplete-or-lost-coverage");
        if (@import("builtin").os.tag == .linux and (!self.native_covered or self.native.failed)) return self.reject("incomplete-directory-event-coverage");
        // Ancestor directory notifications also include unrelated siblings.
        // Revalidate every tracked pathname and absent configuration target,
        // while scanning names only in affected source directories.
        const namespace_dirty = self.dependency_epoch != self.watch.epoch;
        const count = self.watch.event_count;
        for (self.watch.events.items[0..count]) |slot| {
            if (!namespace_dirty) break;
            for (self.routes[slot].items) |path| {
                const witness = self.tokens.get(path) orelse return self.reject("lost-path-route");
                self.path_checks += 1;
                if (!self.watch.identityMatchesPath(witness, path)) return self.reject("changed-path-identity");
                if (self.watch.intact(witness)) continue;
                var checked_dir = std.Io.Dir.cwd().openDir(self.io, path, .{}) catch return self.reject("changed-configuration");
                checked_dir.close(self.io);
                const canonical = std.Io.Dir.cwd().realPathFileAlloc(self.io, path, self.budget.allocator()) catch return self.reject("missing-ancestor-or-dependency");
                defer self.budget.allocator().free(canonical);
                const named_paths = self.native.pathsInDirectory(canonical);
                const named = @import("builtin").os.tag == .linux and self.native_covered and !self.native.failed and self.inventories.contains(canonical) and named_paths.len != 0;
                if (named) {
                    if (!self.allow_membership) {
                        // A vanished temporary name or atomic save need no
                        // enumeration. Names of eligible files must still exist
                        // as regular files; actual membership changes stage.
                        for (named_paths) |event_path| {
                            if (self.native.paths.get(event_path).?) return self.reject("directory-topology-or-metadata");
                            const known = self.sourceIndex(event_path) != null;
                            const stat = std.Io.Dir.cwd().statFile(self.io, event_path, .{ .follow_symlinks = false }) catch |err| {
                                if ((err == error.FileNotFound or err == error.NotDir) and !known) continue;
                                return self.reject("named-directory-invalidation");
                            };
                            if (!known or stat.kind != .file) return self.reject("named-directory-invalidation");
                        }
                    }
                    var recorded = false;
                    for (self.membership_dirs.items) |pending| if (std.mem.eql(u8, pending.path, canonical)) {
                        recorded = true;
                        break;
                    };
                    if (self.allow_membership and !recorded) self.membership_dirs.append(self.budget.allocator(), .{ .path = self.inventories.getKey(canonical).?, .display = self.displays.get(canonical) orelse return self.reject("unrouted-membership"), .stamp = .{}, .native_names = true }) catch return self.reject("membership-budget");
                } else if (self.inventories.get(canonical)) |optional_baseline| {
                    const baseline = optional_baseline orelse return self.reject("uncertified-directory");
                    self.directory_checks += 1;
                    const current = @import("directory_stamp.zig").Stamp.capture(self.io, canonical) catch return self.reject("unstable-directory");
                    if (!baseline.same(current)) {
                        if (!self.allow_membership or baseline.inode != current.inode or baseline.permissions != current.permissions or !std.mem.eql(u8, &baseline.topology, &current.topology)) return self.reject("directory-membership-changed");
                        const display = self.displays.get(canonical) orelse return self.reject("unrouted-membership");
                        var recorded = false;
                        for (self.membership_dirs.items) |pending| if (std.mem.eql(u8, pending.path, canonical)) {
                            if (!pending.stamp.same(current)) return self.reject("racing-membership");
                            recorded = true;
                            break;
                        };
                        if (!recorded) {
                            const stable = self.inventories.getKey(canonical).?;
                            self.membership_dirs.append(self.budget.allocator(), .{ .path = stable, .display = display, .stamp = current }) catch return self.reject("membership-budget");
                        }
                    }
                }
                const checkpoint = self.watch.observed(witness) orelse return self.reject("lost-directory-witness");
                self.approved_sequences[checkpoint.slot] = checkpoint.sequence;
                self.approved_generations[checkpoint.slot] = checkpoint.generation;
            }
            for (self.dependency_routes[slot].items) |path| {
                self.path_checks += 1;
                const present = if (std.Io.Dir.cwd().statFile(self.io, path, .{})) |_| true else |err| blk: {
                    if (err != error.FileNotFound and err != error.NotDir) return self.reject("unreadable-configuration");
                    break :blk false;
                };
                if (present != self.dependencies.get(path).?) return self.reject("configuration-membership-changed");
            }
        }
        var it = self.native.paths.iterator();
        while (it.next()) |event| {
            if (event.value_ptr.*) return self.reject("directory-topology-or-metadata");
            const name = std.fs.path.basename(event.key_ptr.*);
            if (self.allow_membership and (std.mem.eql(u8, name, ".gitignore") or std.mem.eql(u8, name, ".ignore") or std.mem.eql(u8, name, ".rgignore") or std.mem.eql(u8, name, ".git"))) return self.reject("configuration-membership");
            if (self.dependencies.contains(event.key_ptr.*)) return self.reject("configuration");
            if (self.allow_membership) {
                var covered = false;
                for (self.membership_dirs.items) |pending| if (std.mem.eql(u8, pending.path, std.fs.path.dirname(event.key_ptr.*) orelse "/")) {
                    covered = true;
                    break;
                };
                if (covered) continue;
            }
            if (self.sourceIndex(event.key_ptr.*) != null) {
                const stat = std.Io.Dir.cwd().statFile(self.io, event.key_ptr.*, .{}) catch return self.reject("removed-source");
                if (stat.kind != .file) return self.reject("source-kind-changed");
                continue;
            }
            // A temporary atomic-save name may have disappeared before refresh.
            // Existing unknown names can affect discovery/ignore rules.
            _ = std.Io.Dir.cwd().statFile(self.io, event.key_ptr.*, .{}) catch |err| {
                if (err == error.FileNotFound or err == error.NotDir) continue;
                return self.reject("unreadable-path");
            };
            return self.reject("new-membership");
        }
        return true;
    }
    pub fn nativeDirty(self: *Journal) bool {
        self.native.lock();
        defer self.native.unlock();
        return self.native.epoch != self.native_epoch;
    }
    fn insert(self: *Journal, path: []const u8, token: ?Token) void {
        const value = token orelse {
            self.coverage = false;
            return;
        };
        const a = self.budget.allocator();
        const key = a.dupe(u8, path) catch {
            self.coverage = false;
            self.watch.release(value);
            return;
        };
        self.tokens.put(a, key, value) catch {
            self.coverage = false;
            a.free(key);
            self.watch.release(value);
            return;
        };
        self.routes[value.slot].append(a, key) catch self.failMetadata();
        var parent = std.fs.path.dirname(key);
        if (parent == null and !std.mem.eql(u8, key, ".")) parent = ".";
        while (parent) |path_name| {
            if (self.tokens.get(path_name)) |ancestor| if (ancestor.slot != value.slot) {
                self.routes[ancestor.slot].append(a, key) catch self.failMetadata();
            };
            const next = std.fs.path.dirname(path_name);
            if (next != null and std.mem.eql(u8, next.?, path_name)) break;
            parent = next;
        }
    }
    pub fn directory(self: *Journal, path: []const u8) void {
        self.native.track(path);
        self.directoryWitness(path);
    }
    fn directoryWitness(self: *Journal, path: []const u8) void {
        if (!self.coverage or self.tokens.contains(path)) return;
        if (std.fs.path.dirname(path)) |parent| {
            if (!std.mem.eql(u8, parent, path) and parent.len != 0) self.directoryWitness(parent);
        } else if (!std.mem.eql(u8, path, ".")) self.directoryWitness(".");
        if (!self.coverage) return;
        var dir = std.Io.Dir.cwd().openDir(self.io, path, .{}) catch {
            self.coverage = false;
            return;
        };
        defer dir.close(self.io);
        self.insert(path, self.watch.armDirectory(dir));
    }
    pub fn dependency(self: *Journal, path: []const u8) void {
        self.rememberDependency(path, null);
        // Native directory coverage is independent of the bounded inode set.
        // Keep collecting it even when the latter has exhausted its quota.
        // Watch the closest existing parent: creation of missing ancestors or
        // replacement of this path then invalidates the discovery certificate.
        var parent = std.fs.path.dirname(path) orelse ".";
        while (true) {
            var dir = std.Io.Dir.cwd().openDir(self.io, parent, .{}) catch |err| {
                if (err == error.FileNotFound or err == error.NotDir) {
                    parent = std.fs.path.dirname(parent) orelse {
                        self.coverage = false;
                        return;
                    };
                    continue;
                }
                self.coverage = false;
                return;
            };
            dir.close(self.io);
            self.directoryWitness(parent);
            break;
        }
        if (self.tokens.contains(path)) return;
        var file = std.Io.Dir.cwd().openFile(self.io, path, .{}) catch |err| {
            if (err != error.FileNotFound and err != error.NotDir) self.coverage = false else self.rememberDependency(path, false);
            return;
        };
        defer file.close(self.io);
        self.rememberDependency(path, true);
        const canonical = std.Io.Dir.cwd().realPathFileAlloc(self.io, path, self.budget.allocator()) catch {
            self.coverage = false;
            return;
        };
        defer self.budget.allocator().free(canonical);
        self.rememberDependency(canonical, true);
        if (std.fs.path.dirname(canonical)) |actual_parent| self.directoryWitness(actual_parent);
        const stat = file.stat(self.io) catch {
            self.coverage = false;
            return;
        };
        if (stat.kind == .directory) {
            self.directory(path);
            return;
        }
        self.insert(path, self.watch.arm(file));
    }
    pub fn sealGeneration(self: *Journal, sources: *Watch, generation: *const @import("workspace_store.zig").Generation) void {
        self.generation = generation;
        self.seal(sources, generation.entries.items);
    }
    pub fn seal(self: *Journal, sources: *Watch, entries: anytype) void {
        var it = self.tokens.valueIterator();
        while (it.next()) |token| self.watch.certify(token.*);
        self.watch.poll();
        it = self.tokens.valueIterator();
        while (it.next()) |token| if (!self.watch.intact(token.*)) {
            self.coverage = false;
        };
        sources.poll();
        self.sources_covered = sources.fd >= 0 and !sources.failed;
        for (entries) |entry| {
            if (entry.blob.watcher != sources or entry.blob.token == null or !sources.intact(entry.blob.token.?)) self.sources_covered = false;
        }
        if (self.sources_covered) sources.clearEvents();
        self.native_active = !self.sources_covered;
        self.native_covered = !self.native.isFailed() and !self.directory_aliases;
        // Routing stores one canonical/display prefix per directory. The
        // generation's sorted inventory supplies file lookup; no second
        // per-file pathname table or canonicalization pass is retained.
        for (entries) |entry| if (comptime @hasField(@TypeOf(entry), "explicit_file")) {
            const requires_inode = entry.explicit_file or (entry.blob.stamp.valid and entry.blob.stamp.nlink > 1);
            if (requires_inode and (entry.blob.token == null or !sources.intact(entry.blob.token.?))) self.allow_native_sources = false;
        };
        // Events observed during capture are retained, forcing another read.
        self.native.poll();
        self.native.lock();
        if (self.native.paths.count() != 0 or self.native.failed) self.native_covered = false;
        self.native_epoch = self.native.epoch;
        self.native.unlock();
        // Only synchronous inotify queues can extend source coverage. macOS
        // source reuse always requires strict inode witnesses.
        if (@import("builtin").os.tag == .linux and self.native_covered and self.allow_native_sources) self.sources_covered = true;
        self.source_epoch = sources.epoch;
        self.dependency_epoch = self.watch.epoch;
        self.sealed = true;
        if (self.coverage) self.watch.clearEvents();
    }
};

test "journal covers missing optional files, directory membership and lost coverage" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/optional.ignore", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    var journal = Journal.init(std.testing.allocator, io);
    defer journal.deinit();
    if (journal.watch.fd < 0) return error.SkipZigTest;
    var sources = Watch.init();
    defer sources.deinit();
    const Empty = struct { path: []const u8, blob: struct { watcher: ?*Watch, token: ?Token } };
    journal.dependency(path);
    journal.seal(&sources, @as([]const Empty, &.{}));
    if (!journal.clean()) return error.SkipZigTest;
    try tmp.dir.writeFile(io, .{ .sub_path = "optional.ignore", .data = "new-rule" });
    try std.testing.expect(!journal.clean());
    journal.reset();
    journal.dependency(path);
    journal.seal(&sources, @as([]const Empty, &.{}));
    try std.testing.expect(journal.clean());
    try tmp.dir.writeFile(io, .{ .sub_path = "optional.ignore", .data = "modified-rule" });
    try std.testing.expect(!journal.clean());
    journal.reset();
    journal.dependency(path);
    journal.seal(&sources, @as([]const Empty, &.{}));
    try std.testing.expect(journal.clean());
    journal.watch.failClosed();
    try std.testing.expect(!journal.clean());
}

test "directory rearming cannot acknowledge a membership event after validation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    const canonical = try std.Io.Dir.cwd().realPathFileAlloc(io, path, a);
    defer a.free(canonical);
    var journal = Journal.init(a, io);
    defer journal.deinit();
    var sources = Watch.init();
    defer sources.deinit();
    if (journal.watch.fd < 0) return error.SkipZigTest;
    journal.directory(canonical);
    journal.inventory(canonical, try @import("directory_stamp.zig").Stamp.capture(io, canonical));
    const Empty = struct { path: []const u8, blob: struct { watcher: ?*Watch, token: ?Token } };
    journal.seal(&sources, @as([]const Empty, &.{}));
    if (!journal.clean()) return error.SkipZigTest;
    if (@import("builtin").os.tag == .linux and !journal.native_covered) return error.SkipZigTest;
    try tmp.dir.writeFile(io, .{ .sub_path = ".temporary", .data = "save" });
    try tmp.dir.deleteFile(io, ".temporary");
    try std.testing.expect(journal.clean());
    try tmp.dir.writeFile(io, .{ .sub_path = "new.txt", .data = "must be discovered" });
    journal.rearmDirectories();
    try std.testing.expect(!journal.clean());
    // Old sequence approvals must not apply to reused slots after reset.
    journal.reset();
    journal.directory(canonical);
    journal.inventory(canonical, try @import("directory_stamp.zig").Stamp.capture(io, canonical));
    journal.seal(&sources, @as([]const Empty, &.{}));
    try tmp.dir.writeFile(io, .{ .sub_path = "after-reset.txt", .data = "new generation" });
    journal.rearmDirectories();
    try std.testing.expect(!journal.clean());
}
