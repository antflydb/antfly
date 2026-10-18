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

//! Immutable, bounded workspace generations. Capture owns every byte; query
//! workers borrow a generation until joined. Publication is transactional.
const std = @import("std");
const A = std.mem.Allocator;
const Options = @import("options.zig").Options;
const source = @import("source_cache.zig");
const Watch = @import("source_watch.zig").Watch;
const Token = @import("source_watch.zig").Token;
const Budget = @import("antfly_regex").Budget;
pub const Entry = struct {
    blob: *Blob,
    path: []const u8,
    bytes: []const u8,
    revision: [64]u8,
    explicit_file: bool = false,
    prepared: ?source.Prepared = null,
    preparation: ?*PreparedOwner = null,
    fn retain(self: Entry) void {
        self.blob.refs += 1;
        if (self.preparation) |owner| owner.refs += 1;
    }
    fn release(self: Entry) void {
        self.blob.release();
        if (self.preparation) |owner| owner.release();
    }
    fn charge(self: Entry) usize {
        return self.blob.budget.live + @sizeOf(Blob) + (if (self.preparation) |owner| owner.bytes() else @as(usize, 0));
    }
};
pub const Generation = struct {
    refs: usize = 1,
    backing: A,
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    entries: std.ArrayList(Entry) = .empty,
    revision: [64]u8 = undefined,
    captured_at: i64 = 0,
    source_bytes: usize = 0,
    watch_fallback: bool = false,
    source_bytes_read: usize = 0,
    sources_opened: usize = 0,
    discovery_reused: bool = false,
    source_bytes_reused: usize = 0,
    preparations_built: usize = 0,
    preparations_reused: usize = 0,
    parent_revision: ?[64]u8 = null,
    changes: std.ArrayList([]const u8) = .empty,
    blob_bytes: usize = 0,
    merkle: [][32]u8 = &.{},
    merkle_base: usize = 0,
    watch_heads: []usize = &.{},
    watch_next: []usize = &.{},
    watch_slots: []usize = &.{},
    touched: []u64 = &.{},
    touch_epoch: u64 = 0,
    refresh_entries_visited: usize = 0,
    preparation_mutex: std.Io.Mutex = .init,
    preparation_count: usize = 0,
    pub fn capture(a: A, io: std.Io, o: *const Options, previous: ?*const Generation) !*Generation {
        return captureWatched(a, io, o, previous, null);
    }
    pub fn captureWatched(a: A, io: std.Io, o: *const Options, previous: ?*const Generation, watcher: ?*Watch) !*Generation {
        if (watcher) |w| w.beginRefresh();
        return captureOnce(a, io, o, previous, watcher, null, false) catch |err| {
            if (watcher) |w| if (w.fd >= 0) {
                // Witness resources must never turn a capture that fits without
                // watchers into an error. Retry once with every witness lost.
                w.deinit();
                const next = try captureOnce(a, io, o, previous, watcher, null, false);
                next.watch_fallback = true;
                return next;
            };
            return err;
        };
    }
    pub fn captureJournal(a: A, io: std.Io, o: *const Options, previous: ?*Generation, watcher: ?*Watch, journal: ?*@import("discovery_journal.zig").Journal) !*Generation {
        const w = watcher orelse return captureWatched(a, io, o, previous, null);
        const j = journal orelse return captureWatched(a, io, o, previous, w);
        w.beginRefresh();
        const clean = previous != null and j.clean();
        if (clean and j.sources_covered and !w.failed and w.fd >= 0 and j.source_epoch == w.epoch) {
            const old = previous.?;
            old.refs += 1;
            old.captured_at = std.Io.Clock.real.now(io).toSeconds();
            old.source_bytes_read = 0;
            old.sources_opened = 0;
            old.source_bytes_reused = old.source_bytes;
            old.preparations_reused += old.preparations_built;
            old.preparations_built = 0;
            old.discovery_reused = true;
            old.refresh_entries_visited = 0;
            return old;
        }
        if (clean and j.sources_covered and previous.?.refs == 1 and !w.failed) {
            if (captureDirty(a, io, o, previous.?, w, j) catch null) |updated| return updated;
        }
        if (!clean) j.reset();
        const next = captureOnce(a, io, o, previous, w, j, clean) catch {
            w.deinit();
            j.deinit();
            j.sealed = false;
            j.coverage = false;
            j.sources_covered = false;
            const fallback = try captureOnce(a, io, o, previous, w, null, false);
            fallback.watch_fallback = true;
            return fallback;
        };
        j.seal(w, next.entries.items);
        return next;
    }
    fn captureOnce(a: A, io: std.Io, o: *const Options, previous: ?*const Generation, watcher: ?*Watch, journal: ?*@import("discovery_journal.zig").Journal, trusted: bool) !*Generation {
        if (o.workspace_memory_limit < @sizeOf(Generation)) return error.OutOfMemory;
        const self = try a.create(Generation);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = o.workspace_memory_limit - @sizeOf(Generation) }, .arena = undefined };
        self.arena = .init(self.budget.allocator());
        errdefer self.deinit();
        var collector = Collector{ .generation = self, .io = io, .options = o, .previous = previous, .watcher = watcher, .journal = journal, .trusted_membership = trusted };
        self.discovery_reused = trusted;
        if (trusted) {
            for (previous.?.entries.items) |entry| try collector.addFile(entry.path, entry.path, entry.explicit_file);
        } else if (o.paths.items.len == 0) try collector.path(".") else for (o.paths.items) |path| try collector.path(path);
        std.mem.sort(Entry, self.entries.items, {}, struct {
            fn less(_: void, x: Entry, y: Entry) bool {
                return std.mem.order(u8, x.path, y.path) == .lt;
            }
        }.less);
        self.merkle_base = std.math.ceilPowerOfTwo(usize, @max(1, self.entries.items.len)) catch return error.OutOfMemory;
        self.merkle = try self.arena.allocator().alloc([32]u8, 2 * self.merkle_base);
        @memset(self.merkle, @as([32]u8, @splat(0)));
        for (self.entries.items, 0..) |entry, i| {
            if (i > 0 and std.mem.eql(u8, self.entries.items[i - 1].path, entry.path)) return error.DuplicateWorkspacePath;
            self.merkle[self.merkle_base + i] = leafHash(entry);
        }
        var branch = self.merkle_base;
        while (branch > 1) {
            branch -= 1;
            self.merkle[branch] = branchHash(self.merkle[branch * 2], self.merkle[branch * 2 + 1]);
        }
        self.revision = rootRevision(self.entries.items.len, self.merkle[1]);
        if (watcher != null and journal != null) {
            const allocator = self.arena.allocator();
            self.watch_heads = try allocator.alloc(usize, watcher.?.limit);
            @memset(self.watch_heads, std.math.maxInt(usize));
            self.watch_next = try allocator.alloc(usize, self.entries.items.len);
            self.watch_slots = try allocator.alloc(usize, self.entries.items.len);
            self.touched = try allocator.alloc(u64, self.entries.items.len);
            @memset(self.touched, 0);
            for (self.entries.items, 0..) |entry, i| {
                self.watch_next[i] = std.math.maxInt(usize);
                self.watch_slots[i] = std.math.maxInt(usize);
                if (entry.blob.token) |token| self.linkWatch(i, token.slot);
            }
        }
        self.refresh_entries_visited = self.entries.items.len;
        if (previous) |old| {
            self.parent_revision = old.revision;
            if (std.mem.eql(u8, &self.revision, &old.revision)) {
                self.parent_revision = old.parent_revision;
                for (old.changes.items) |path| try self.changes.append(self.arena.allocator(), try self.arena.allocator().dupe(u8, path));
            }
            var left: usize = 0;
            var right: usize = 0;
            while (left < old.entries.items.len or right < self.entries.items.len) {
                const order: std.math.Order = if (left == old.entries.items.len) .gt else if (right == self.entries.items.len) .lt else std.mem.order(u8, old.entries.items[left].path, self.entries.items[right].path);
                const path = if (order == .lt) old.entries.items[left].path else self.entries.items[right].path;
                if (order != .eq or !std.mem.eql(u8, &old.entries.items[left].revision, &self.entries.items[right].revision) or old.entries.items[left].explicit_file != self.entries.items[right].explicit_file)
                    try self.changes.append(self.arena.allocator(), try self.arena.allocator().dupe(u8, path));
                if (order != .gt) left += 1;
                if (order != .lt) right += 1;
            }
        }
        self.captured_at = std.Io.Clock.real.now(io).toSeconds();
        return self;
    }
    fn leafHash(entry: Entry) [32]u8 {
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update("antfly-workspace-leaf-v2");
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, entry.path.len, .little);
        h.update(&length);
        h.update(entry.path);
        h.update(&entry.revision);
        h.update(&.{@intFromBool(entry.explicit_file)});
        var digest: [32]u8 = undefined;
        h.final(&digest);
        return digest;
    }
    fn branchHash(left: [32]u8, right: [32]u8) [32]u8 {
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update("antfly-workspace-branch-v2");
        h.update(&left);
        h.update(&right);
        var digest: [32]u8 = undefined;
        h.final(&digest);
        return digest;
    }
    fn rootRevision(count: usize, root: [32]u8) [64]u8 {
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update("antfly-workspace-root-v2");
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, count, .little);
        h.update(&length);
        h.update(&root);
        var digest: [32]u8 = undefined;
        h.final(&digest);
        return std.fmt.bytesToHex(digest, .lower);
    }
    fn linkWatch(self: *Generation, i: usize, slot: usize) void {
        self.watch_next[i] = self.watch_heads[slot];
        self.watch_heads[slot] = i;
        self.watch_slots[i] = slot;
    }
    fn unlinkWatch(self: *Generation, i: usize) void {
        const slot = self.watch_slots[i];
        if (slot == std.math.maxInt(usize)) return;
        var link = &self.watch_heads[slot];
        while (link.* != std.math.maxInt(usize)) {
            if (link.* == i) {
                link.* = self.watch_next[i];
                break;
            }
            link = &self.watch_next[link.*];
        }
    }
    /// Query workers have joined and the namespace certificate is unchanged.
    /// Stage dirty contents and Merkle paths before touching the published tree.
    /// Exclusive ownership permits transactional replacement without retaining
    /// a chain of prior generations or copying the unchanged inventory.
    fn captureDirty(a: A, io: std.Io, o: *const Options, old: *Generation, w: *Watch, journal: *@import("discovery_journal.zig").Journal) !?*Generation {
        if (old.watch_heads.len == 0 or old.watch_next.len != old.entries.items.len or old.touch_epoch == std.math.maxInt(u64)) return null;
        const epoch = w.epoch;
        const original_limit = old.budget.limit;
        var published = false;
        defer if (!published) {
            old.budget.limit = original_limit;
        };
        old.touch_epoch += 1;
        var stage: Generation = .{ .backing = a, .budget = .{ .backing = a, .limit = o.workspace_memory_limit - @sizeOf(Generation) }, .arena = undefined };
        stage.arena = .init(stage.budget.allocator());
        defer {
            for (stage.entries.items) |entry| entry.release();
            stage.arena.deinit();
        }
        var indices: std.ArrayList(usize) = .empty;
        var updates: std.AutoHashMapUnmanaged(usize, [32]u8) = .empty;
        const scratch = stage.arena.allocator();
        // Freeze the event suffix; any new event during capture abandons the
        // optimization before publication and uses conservative capture.
        const event_count = w.event_count;
        for (w.events[0..event_count]) |slot| {
            var i = old.watch_heads[slot];
            while (i != std.math.maxInt(usize)) : (i = old.watch_next[i]) {
                if (old.touched[i] == old.touch_epoch) continue;
                old.touched[i] = old.touch_epoch;
                try indices.append(scratch, i);
            }
        }
        if (indices.items.len == 0) return null;
        var collector: Collector = .{ .generation = &stage, .io = io, .options = o, .previous = old, .watcher = w };
        var retained = old.blob_bytes;
        var source_bytes = old.source_bytes;
        var changed: usize = 0;
        for (indices.items) |i| {
            const entry = old.entries.items[i];
            const before = stage.entries.items.len;
            try collector.addFile(entry.path, entry.path, entry.explicit_file);
            // Oversize exclusion changes membership, requiring ordinary capture.
            if (stage.entries.items.len != before + 1) return null;
            const next = stage.entries.items[before];
            source_bytes = source_bytes - entry.bytes.len + next.bytes.len;
            retained = retained - entry.charge() + next.charge();
            if (!std.mem.eql(u8, &entry.revision, &next.revision)) changed += 1;
            var node = old.merkle_base + i;
            try updates.put(scratch, node, leafHash(next));
            while (node > 1) {
                node /= 2;
                const left = updates.get(node * 2) orelse old.merkle[node * 2];
                const right = updates.get(node * 2 + 1) orelse old.merkle[node * 2 + 1];
                try updates.put(scratch, node, branchHash(left, right));
            }
        }
        if (retained + old.budget.live + @sizeOf(Generation) > o.workspace_memory_limit) return null;
        const revision = rootRevision(old.entries.items.len, updates.get(1) orelse old.merkle[1]);
        if (!std.mem.eql(u8, &revision, &old.revision)) {
            // Restore the old blob allowance only for bounded metadata growth.
            const prior_limit = old.budget.limit;
            old.budget.limit = @min(original_limit, o.workspace_memory_limit - @sizeOf(Generation) - retained);
            old.changes.ensureTotalCapacity(old.arena.allocator(), changed) catch |err| {
                old.budget.limit = prior_limit;
                return err;
            };
        }
        w.poll();
        if (!journal.clean() or w.failed or w.epoch != epoch) return null;
        for (stage.entries.items) |entry| if (entry.blob.token == null or !w.intact(entry.blob.token.?)) return null;
        // No fallible work remains after this point.
        const content_changed = !std.mem.eql(u8, &revision, &old.revision);
        if (content_changed) {
            old.parent_revision = old.revision;
            old.changes.clearRetainingCapacity();
        }
        old.preparations_built = 0;
        old.preparations_reused = old.preparation_count;
        for (indices.items, stage.entries.items) |i, next| {
            const prior = old.entries.items[i];
            if (content_changed and !std.mem.eql(u8, &prior.revision, &next.revision)) old.changes.appendAssumeCapacity(prior.path);
            old.unlinkWatch(i);
            old.entries.items[i] = next;
            old.entries.items[i].path = prior.path;
            next.retain();
            old.preparation_count = old.preparation_count - @intFromBool(prior.prepared != null) + @intFromBool(next.prepared != null);
            prior.release();
            if (next.blob.token) |token| old.linkWatch(i, token.slot);
        }
        var it = updates.iterator();
        while (it.next()) |item| old.merkle[item.key_ptr.*] = item.value_ptr.*;
        old.preparations_reused = old.preparation_count;
        old.blob_bytes = retained;
        old.budget.limit = o.workspace_memory_limit - @sizeOf(Generation) - retained;
        old.revision = revision;
        old.source_bytes = source_bytes;
        old.captured_at = std.Io.Clock.real.now(io).toSeconds();
        old.sources_opened = stage.sources_opened;
        old.source_bytes_read = stage.source_bytes_read;
        old.source_bytes_reused = old.source_bytes -| stage.source_bytes_read;
        old.discovery_reused = true;
        old.refresh_entries_visited = indices.items.len;
        old.refs += 1;
        published = true;
        w.clearEvents();
        journal.source_epoch = w.epoch;
        return old;
    }
    /// Immutable content has an optional, independently owned derived memo.
    /// Reserve its peak before parsing, then publish only a complete preparation.
    /// Workers prepare different entries concurrently; metadata admission alone
    /// takes the generation mutex. Failure is an ordinary uncached-query fallback.
    pub fn prepareEntry(generation: *const Generation, io: std.Io, entry: *const Entry) bool {
        const self = @constCast(generation);
        if (entry.prepared != null or !std.unicode.utf8ValidateSlice(entry.bytes) or std.mem.indexOfScalar(u8, entry.bytes, 0) != null or @import("antfly_encoding").hasBom(entry.bytes)) return false;
        self.preparation_mutex.lockUncancelable(io);
        const available = self.budget.limit -| self.budget.live;
        const desired = @max(2 * 1024 * 1024, @min(32 * 1024 * 1024, std.math.mul(usize, entry.bytes.len, 16) catch std.math.maxInt(usize)));
        const reservation = @min(available, desired);
        if (reservation <= @sizeOf(PreparedOwner)) {
            self.preparation_mutex.unlock(io);
            return false;
        }
        self.budget.limit -= reservation;
        self.preparation_mutex.unlock(io);
        const owner = PreparedOwner.create(self.backing, reservation, entry.path, entry.bytes) catch {
            self.preparation_mutex.lockUncancelable(io);
            self.budget.limit += reservation;
            self.preparation_mutex.unlock(io);
            return false;
        };
        self.preparation_mutex.lockUncancelable(io);
        defer self.preparation_mutex.unlock(io);
        const size = owner.bytes();
        const mutable = @constCast(entry);
        mutable.preparation = owner;
        mutable.prepared = owner.value;
        self.budget.limit += reservation - size;
        self.blob_bytes += size;
        self.preparation_count += 1;
        self.preparations_built += 1;
        return true;
    }
    /// Shared blobs count in full against every generation that retains them.
    pub fn retainedBytes(self: *const Generation) usize {
        return self.budget.live + @sizeOf(Generation) + self.blob_bytes;
    }
    pub fn deinit(self: *Generation) void {
        self.refs -= 1;
        if (self.refs != 0) return;
        const a = self.backing;
        for (self.entries.items) |entry| entry.release();
        self.arena.deinit();
        a.destroy(self);
    }
    pub fn find(self: *const Generation, path_name: []const u8) ?*const Entry {
        var begin: usize = 0;
        var end = self.entries.items.len;
        while (begin < end) {
            const mid = begin + (end - begin) / 2;
            const entry = &self.entries.items[mid];
            switch (std.mem.order(u8, entry.path, path_name)) {
                .eq => return entry,
                .lt => begin = mid + 1,
                .gt => end = mid,
            }
        }
        return null;
    }
};
/// Independent allocations never borrow a generation allocator. Refresh can
/// release the old generation immediately, including after arbitrarily many reuses.
const Blob = struct {
    backing: A,
    refs: usize = 1, // capture/publication run serially after query workers join
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    bytes: []const u8 = "",
    stamp: ?std.Io.File.Stat = null,
    watcher: ?*Watch = null,
    token: ?Token = null,
    fn release(self: *Blob) void {
        self.refs -= 1;
        if (self.refs != 0) return;
        const a = self.backing;
        if (self.watcher) |watcher| if (self.token) |token| watcher.release(token);
        self.arena.deinit();
        a.destroy(self);
    }
};
const PreparedOwner = struct {
    backing: A,
    refs: usize = 1,
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    value: source.Prepared,
    fn create(a: A, maximum: usize, path: []const u8, bytes_value: []const u8) !*PreparedOwner {
        const self = try a.create(PreparedOwner);
        errdefer a.destroy(self);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = maximum - @sizeOf(PreparedOwner) }, .arena = undefined, .value = undefined };
        self.arena = .init(self.budget.allocator());
        errdefer self.arena.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const value: source.Prepared = .{ .lines = try source.lines(temporary, bytes_value, '\n', false), .index = try @import("source.zig").parse(temporary, path, bytes_value) };
        self.value = try clonePrepared(self.arena.allocator(), temporary, bytes_value, value);
        return self;
    }
    fn bytes(self: *const PreparedOwner) usize {
        return @sizeOf(PreparedOwner) + self.budget.live;
    }
    fn release(self: *PreparedOwner) void {
        self.refs -= 1;
        if (self.refs != 0) return;
        const a = self.backing;
        self.arena.deinit();
        a.destroy(self);
    }
};
fn clonePrepared(a: A, temporary: A, bytes: []const u8, value: source.Prepared) !source.Prepared {
    var out = value;
    out.lines = try a.dupe(source.Line, value.lines);
    out.index.declarations = try a.dupe(@import("source.zig").Declaration, value.index.declarations);
    out.index.calls = try a.dupe(@import("source.zig").Call, value.index.calls);
    // Most identifiers are views into the captured bytes. Preserve those
    // offsets instead of allocating a second copy or a large name hash table.
    // Parser-synthesized names still need independent ownership.
    var kinds: std.StringHashMapUnmanaged([]const u8) = .empty;
    defer kinds.deinit(temporary);
    inline for (.{ "declarations", "calls" }) |field| {
        for (@field(out.index, field)) |*item| {
            const start = @intFromPtr(item.name.ptr);
            const base = @intFromPtr(bytes.ptr);
            if (start >= base and start - base <= bytes.len and item.name.len <= bytes.len - (start - base)) {
                item.name = bytes[start - base ..][0..item.name.len];
            } else item.name = try a.dupe(u8, item.name);
            const entry = try kinds.getOrPut(temporary, item.kind);
            if (!entry.found_existing) {
                entry.value_ptr.* = try a.dupe(u8, item.kind);
                entry.key_ptr.* = entry.value_ptr.*;
            }
            item.kind = entry.value_ptr.*;
        }
    }
    return out;
}
const Collector = struct {
    generation: *Generation,
    io: std.Io,
    options: *const Options,
    previous: ?*const Generation,
    watcher: ?*Watch = null,
    journal: ?*@import("discovery_journal.zig").Journal = null,
    trusted_membership: bool = false,
    fn path(self: *Collector, name: []const u8) !void {
        if (std.mem.eql(u8, name, "-")) return error.WorkspaceStdinUnsupported;
        var scratch = std.heap.ArenaAllocator.init(self.generation.budget.allocator());
        defer scratch.deinit();
        const a = scratch.allocator();
        if (self.journal) |journal| journal.dependency(name);
        const info = try std.Io.Dir.cwd().statFile(self.io, name, .{});
        if (info.kind == .directory) {
            const full = try std.Io.Dir.cwd().realPathFileAlloc(self.io, name, a);
            var discovery = @import("discovery.zig").Discovery{ .a = a, .io = self.io, .options = self.options, .journal = self.journal };
            defer discovery.deinit();
            try discovery.prepare(full);
            try discovery.walk(full, if (std.mem.eql(u8, name, ".") and self.options.paths.items.len == 0) "" else name, self);
        } else if (info.kind == .file) try self.addFile(name, name, true) else return error.NotRegularFile;
    }
    pub fn stopped(_: *Collector) bool {
        return false;
    }
    pub fn reportError(_: *Collector, _: []const u8, err: anyerror) anyerror!void {
        // An incomplete capture must never replace the published generation.
        return err;
    }
    pub fn file(self: *Collector, path_name: []const u8, display: []const u8) !void {
        return self.addFile(path_name, display, false);
    }
    fn addFile(self: *Collector, path_name: []const u8, display: []const u8, explicit_file: bool) !void {
        const generation = self.generation;
        if (generation.entries.items.len >= self.options.workspace_max_files) return error.WorkspaceFileLimit;
        if (self.trusted_membership) if (self.watcher) |watcher| if (if (self.previous) |old| old.find(display) else null) |old| {
            if (old.blob.watcher == watcher and old.blob.token != null and watcher.intact(old.blob.token.?)) {
                const available = generation.budget.limit -| generation.budget.live;
                old.blob.refs += 1;
                errdefer old.blob.release();
                try self.append(old.blob, display, explicit_file, old.revision, true, available);
                generation.source_bytes_reused += old.bytes.len;
                return;
            }
        };
        generation.sources_opened += 1;
        var file_handle = try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
        defer file_handle.close(self.io);
        const before = try file_handle.stat(self.io);
        if (before.kind != .file) return error.NotRegularFile;
        if (before.size > self.options.max_file_bytes and self.options.skip_oversize) return;
        const available = generation.budget.limit -| generation.budget.live;
        if (available < @sizeOf(Blob)) return error.OutOfMemory;
        if (self.watcher) |watcher| if (if (self.previous) |old| old.find(display) else null) |old| {
            if (old.blob.token != null and old.blob.watcher == watcher and old.blob.stamp != null and sameStamp(old.blob.stamp.?, before) and watcher.clean(old.blob.token.?, file_handle)) {
                if (old.blob.budget.live + @sizeOf(Blob) > available) return error.OutOfMemory;
                old.blob.refs += 1;
                errdefer old.blob.release();
                try self.append(old.blob, display, explicit_file, old.revision, true, available);
                generation.source_bytes_reused += old.bytes.len;
                return;
            }
        };
        var token = if (self.watcher) |w| w.arm(file_handle) else null;
        defer if (token) |value| self.watcher.?.release(value);
        var blob = try generation.backing.create(Blob);
        // Use the ordinary search allocator for parser/capture arenas so
        // bounded parser outcomes remain identical across both entry points.
        blob.* = .{ .backing = generation.backing, .budget = .{ .backing = generation.backing, .limit = available - @sizeOf(Blob) }, .arena = undefined };
        blob.arena = .init(blob.budget.allocator());
        errdefer blob.release();
        var buffer: [65536]u8 = undefined;
        var reader = file_handle.reader(self.io, &buffer);
        blob.bytes = try reader.interface.allocRemaining(blob.arena.allocator(), .limited(@min(self.options.max_file_bytes, self.options.workspace_memory_limit)));
        const after = try file_handle.stat(self.io);
        if (before.inode != after.inode or before.size != after.size or after.size != blob.bytes.len or
            before.mtime.nanoseconds != after.mtime.nanoseconds or before.ctime.nanoseconds != after.ctime.nanoseconds) return error.WorkspaceChangedDuringCapture;
        generation.source_bytes_read += blob.bytes.len;
        if (token) |value| self.watcher.?.certify(value);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(blob.bytes, &digest, .{});
        const revision = std.fmt.bytesToHex(digest, .lower);
        var reused = false;
        if (if (self.previous) |old| old.find(display) else null) |old| {
            if (std.mem.eql(u8, &old.revision, &revision)) {
                const retained = old.blob.budget.live + @sizeOf(Blob);
                if (retained > available) return error.OutOfMemory;
                blob.release();
                blob = old.blob;
                blob.refs += 1;
                reused = true;
            }
        }
        if (blob.watcher) |w| if (blob.token) |old| w.release(old);
        blob.watcher = self.watcher;
        blob.token = token;
        blob.stamp = before;
        token = null;
        try self.append(blob, display, explicit_file, revision, reused, available);
    }
    fn sameStamp(a: std.Io.File.Stat, b: std.Io.File.Stat) bool {
        return a.inode == b.inode and a.kind == b.kind and a.size == b.size and a.mtime.nanoseconds == b.mtime.nanoseconds and a.ctime.nanoseconds == b.ctime.nanoseconds;
    }
    fn append(self: *Collector, blob: *Blob, display: []const u8, explicit_file: bool, revision: [64]u8, reused: bool, available: usize) !void {
        const generation = self.generation;
        const a = generation.arena.allocator();
        const previous = if (self.previous) |old| old.find(display) else null;
        const preparation = if (previous != null and std.mem.eql(u8, &previous.?.revision, &revision)) previous.?.preparation else null;
        const charge = blob.budget.live + @sizeOf(Blob) + (if (preparation) |owner| owner.bytes() else @as(usize, 0));
        if (charge > available) return error.OutOfMemory;
        generation.budget.limit -= charge;
        generation.blob_bytes += charge;
        errdefer {
            generation.budget.limit += charge;
            generation.blob_bytes -= charge;
        }
        try generation.entries.append(a, .{ .blob = blob, .path = try a.dupe(u8, display), .bytes = blob.bytes, .revision = revision, .explicit_file = explicit_file, .prepared = if (preparation) |owner| owner.value else null, .preparation = preparation });
        generation.source_bytes += blob.bytes.len;
        if (preparation) |owner| {
            owner.refs += 1;
            generation.preparations_reused += 1;
            generation.preparation_count += 1;
        }
        _ = reused;
    }
};

test "workspace generations own reused indexes and failed captures release their quota" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a.py", .data = "def retry():\n    STOP()\n" });
    const path_name = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path_name);
    var o: Options = .{ .no_ignore = true };
    defer o.deinit(a);
    try o.paths.append(a, path_name);
    var old: ?*Generation = try Generation.capture(a, io, &o, null);
    defer if (old) |value| value.deinit();
    try std.testing.expectEqual(@as(usize, 0), old.?.preparations_built);
    try std.testing.expect(old.?.prepareEntry(io, &old.?.entries.items[0]));
    const next = try Generation.capture(a, io, &o, old);
    defer next.deinit();
    try std.testing.expectEqualSlices(u8, &old.?.revision, &next.revision);
    try std.testing.expectEqual(@as(usize, 0), next.preparations_built);
    try std.testing.expectEqual(@as(usize, 1), next.preparations_reused);
    try std.testing.expect(old.?.entries.items[0].blob == next.entries.items[0].blob);
    try std.testing.expect(next.retainedBytes() <= o.workspace_memory_limit);
    old.?.deinit();
    old = null;
    try std.testing.expectEqualStrings("retry", next.entries.items[0].prepared.?.index.declarations[0].name);
    try std.testing.expectEqualStrings("STOP", next.entries.items[0].prepared.?.index.calls[0].name);
    o.workspace_max_files = 0;
    try std.testing.expectError(error.WorkspaceFileLimit, Generation.capture(a, io, &o, next));
    o.workspace_max_files = 10;
    o.workspace_memory_limit = 1;
    try std.testing.expectError(error.OutOfMemory, Generation.capture(a, io, &o, next));
}

test "preparations preserve source name offsets and own parser synthesized names" {
    const a = std.testing.allocator;
    var retained = std.heap.ArenaAllocator.init(a);
    defer retained.deinit();
    const bytes = try retained.allocator().dupe(u8, "def retry():\n    STOP()\n");
    var scratch = std.heap.ArenaAllocator.init(a);
    var scratch_live = true;
    defer if (scratch_live) scratch.deinit();
    const temp = scratch.allocator();
    var calls = [_]@import("source.zig").Call{.{ .name = try temp.dupe(u8, "Receiver.method"), .line = 2, .column = 1, .start_byte = 13, .end_byte = 20 }};
    var declarations = [_]@import("source.zig").Declaration{.{ .name = bytes[4..9], .kind = "function", .start = 1, .end = 2, .label_line = 1, .start_byte = 0, .end_byte = bytes.len }};
    const index = try clonePrepared(retained.allocator(), temp, bytes, .{ .lines = try source.lines(temp, bytes, '\n', false), .index = .{ .declarations = &declarations, .calls = &calls, .complete = true } });
    scratch.deinit();
    scratch_live = false;
    try std.testing.expect(index.index.declarations[0].name.ptr == bytes[4..9].ptr);
    try std.testing.expectEqualStrings("Receiver.method", index.index.calls[0].name);
}

test "dirty capture preserves a retained generation and lazy preparations stay bounded" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a.py", .data = "def old():\n    STOP()\n" });
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    var o: Options = .{ .no_ignore = true };
    defer o.deinit(a);
    try o.paths.append(a, path);
    var watch = Watch.init();
    defer watch.deinit();
    var journal = @import("discovery_journal.zig").Journal.init(a, io);
    defer journal.deinit();
    const old = try Generation.captureJournal(a, io, &o, null, &watch, &journal);
    defer old.deinit();
    old.refs += 1;
    defer old.deinit();
    const revision = old.revision;
    try tmp.dir.writeFile(io, .{ .sub_path = "a.py", .data = "def changed():\n    STOP()\n" });
    const next = try Generation.captureJournal(a, io, &o, old, &watch, &journal);
    defer next.deinit();
    try std.testing.expect(old != next);
    try std.testing.expectEqualSlices(u8, &revision, &old.revision);
    try std.testing.expectEqualStrings("def old():\n    STOP()\n", old.entries.items[0].bytes);
    try std.testing.expectEqualStrings("def changed():\n    STOP()\n", next.entries.items[0].bytes);
    try std.testing.expectEqual(@as(usize, 0), next.preparations_built);
    try std.testing.expect(next.prepareEntry(io, &next.entries.items[0]));
    try std.testing.expect(next.retainedBytes() <= o.workspace_memory_limit);
    try std.testing.expect(old.entries.items[0].prepared == null);
    try std.testing.expectEqualStrings("changed", next.entries.items[0].prepared.?.index.declarations[0].name);
}
