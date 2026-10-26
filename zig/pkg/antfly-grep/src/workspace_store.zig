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
const inventory = @import("inventory.zig");
pub const Entry = struct {
    blob: *Blob,
    path: []const u8,
    digest: [32]u8,
    explicit_file: bool = false,
    witness_reused: bool = false,
    watch_next: usize = std.math.maxInt(usize),
    watch_prev: usize = std.math.maxInt(usize),
    watch_slot: usize = std.math.maxInt(usize),
    touched: u64 = 0,
    directory_next: usize = std.math.maxInt(usize),
    directory_prev: usize = std.math.maxInt(usize),
    preparation: ?*PreparedOwner = null,
    pub fn bytes(self: Entry) []const u8 {
        return self.blob.bytes;
    }
    pub fn revision(self: Entry) [64]u8 {
        return std.fmt.bytesToHex(self.digest, .lower);
    }
    pub fn prepared(self: Entry) ?source.Prepared {
        return if (self.preparation) |owner| owner.value else null;
    }
    fn retain(self: Entry) void {
        self.blob.refs += 1;
        if (self.preparation) |owner| owner.refs += 1;
    }
    fn release(self: Entry) void {
        self.blob.release();
        if (self.preparation) |owner| owner.release();
    }
    fn charge(self: Entry) usize {
        return self.blob.charge() + (if (self.preparation) |owner| owner.bytes() else @as(usize, 0));
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
    path_bytes: usize = 0,
    watch_fallback: bool = false,
    source_bytes_read: usize = 0,
    capture_workers: usize = 0,
    parallel_sources_read: usize = 0,
    parallel_sources_opened: usize = 0,
    sources_opened: usize = 0,
    discovery_reused: bool = false,
    source_bytes_reused: usize = 0,
    preparations_built: usize = 0,
    preparations_reused: usize = 0,
    parent_revision: ?[64]u8 = null,
    changes: std.ArrayList([]const u8) = .empty,
    blob_bytes: usize = 0,
    tree: ?*inventory.Node = null,
    watch_heads: []usize = &.{},
    directory_heads: std.StringHashMapUnmanaged(usize) = .empty,
    touch_epoch: u64 = 0,
    refresh_entries_visited: usize = 0,
    namespace_paths_checked: usize = 0,
    namespace_directories_scanned: usize = 0,
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
        if (journal) |j| {
            j.path_checks = 0;
            j.directory_checks = 0;
        }
        const next = try captureJournalInner(a, io, o, previous, watcher, journal);
        if (previous != null) {
            next.capture_workers = 0;
            next.parallel_sources_read = 0;
            next.parallel_sources_opened = 0;
        }
        if (journal) |j| {
            next.namespace_paths_checked = j.path_checks;
            next.namespace_directories_scanned = j.directory_checks;
        }
        return next;
    }
    fn captureJournalInner(a: A, io: std.Io, o: *const Options, previous: ?*Generation, watcher: ?*Watch, journal: ?*@import("discovery_journal.zig").Journal) !*Generation {
        const w = watcher orelse return captureWatched(a, io, o, previous, null);
        const j = journal orelse return captureWatched(a, io, o, previous, w);
        if (o.trace) {
            j.native.lock();
            j.native.trace = true;
            j.native.unlock();
        }
        j.allow_native_sources = !o.follow;
        w.beginRefresh();
        const clean = previous != null and j.clean();
        if (clean and j.sources_covered and !w.failed and w.fd >= 0 and j.source_epoch == w.epoch and (!j.native_active or !j.nativeDirty())) {
            const old = previous.?;
            if (j.native_refresh) {
                j.rearmDirectories();
                if (!j.native_active) j.native.retire(j.native.checkpoint());
            }
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
            // Delayed/coalesced events may arrive while a source is staged.
            // Retry boundedly against the new checkpoint before full capture.
            for (0..3) |_| {
                if (captureDirty(a, io, o, previous.?, w, j) catch null) |updated| return updated;
                w.poll();
                if (!j.clean() or w.failed) break;
            }
        }
        if (previous) |old| if (!clean and old.refs == 1 and j.sources_covered and !w.failed) {
            if (captureMembership(a, io, o, old, w, j) catch null) |updated| return updated;
        };
        return captureFallback(a, io, o, previous, w, j, clean);
    }
    fn captureFallback(a: A, io: std.Io, o: *const Options, previous: ?*Generation, w: *Watch, j: *@import("discovery_journal.zig").Journal, was_clean: bool) !*Generation {
        // A rejected dirty capture may have observed a namespace/configuration
        // event while reading. Never carry its earlier certificate into fallback.
        const clean = was_clean and j.clean();
        if (o.trace) std.debug.print("antfly-grep: refresh fallback trusted={} directory_events={} reason={s}\n", .{ clean, j.native_refresh, j.rejection });
        if (!clean) {
            if (previous == null) j.reset() else j.rediscover();
        } else {
            // Conservative capture rereads every unwitnessed source. Events
            // preceding it are consumed; racing later callbacks stay queued.
            j.native.poll();
            j.native.retire(j.native.checkpoint());
        }
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
        j.sealGeneration(w, next);
        if (o.trace) std.debug.print("antfly-grep: coverage namespace={} sources={} routes={d} metadata={d}/{d} denied={}\n", .{ j.coverage, j.sources_covered, j.routing.items.len, j.budget.live, j.budget.limit, j.budget.denied });
        next.watch_fallback = !j.sources_covered or !j.coverage;
        return next;
    }
    fn captureOnce(a: A, io: std.Io, o: *const Options, previous: ?*const Generation, watcher: ?*Watch, journal: ?*@import("discovery_journal.zig").Journal, trusted: bool) !*Generation {
        if (o.workspace_memory_limit < @sizeOf(Generation)) return error.OutOfMemory;
        const self = try a.create(Generation);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = o.workspace_memory_limit - @sizeOf(Generation) }, .arena = undefined };
        self.arena = .init(self.budget.allocator());
        errdefer self.deinit();
        var collector = Collector{ .generation = self, .io = io, .options = o, .previous = previous, .watcher = watcher, .journal = journal, .trusted_membership = trusted };
        var queue: CaptureQueue = .{ .collector = &collector, .pool = .{ .io = io }, .workers = @min(8, if (o.jobs_auto) @min(4, o.jobs) else o.jobs), .capacity = CaptureQueue.descriptorCapacity() };
        defer queue.deinit();
        var opens: OpenQueue = .{ .collector = &collector, .pool = &queue.pool, .workers = queue.workers, .capacity = @max(1, queue.capacity / 2) };
        defer opens.deinit();
        if (previous == null and queue.workers > 1) {
            collector.queue = &queue;
            if (o.workspace_parallel_open and queue.capacity >= 2) {
                collector.opens = &opens;
                queue.capacity -= opens.capacity;
            }
        }
        self.discovery_reused = trusted;
        if (watcher) |w| w.poll();
        if (trusted) {
            for (previous.?.entries.items) |entry| try collector.addFile(entry.path, entry.path, entry.explicit_file);
        } else if (o.paths.items.len == 0) try collector.path(".") else for (o.paths.items) |path| try collector.path(path);
        try opens.flush();
        try queue.flush();
        if (watcher) |w| {
            w.poll();
            for (self.entries.items) |entry| if (entry.witness_reused and
                (entry.blob.token == null or !w.intact(entry.blob.token.?))) return error.WorkspaceChangedDuringCapture;
        }
        std.mem.sort(Entry, self.entries.items, {}, struct {
            fn less(_: void, x: Entry, y: Entry) bool {
                return std.mem.order(u8, x.path, y.path) == .lt;
            }
        }.less);
        for (self.entries.items, 0..) |entry, i| {
            if (i > 0 and std.mem.eql(u8, self.entries.items[i - 1].path, entry.path)) return error.DuplicateWorkspacePath;
        }
        self.tree = try inventory.build(self.budget.allocator(), self.entries.items, leafHash);
        self.revision = inventoryRevision(self.tree);
        if (watcher != null and journal != null) {
            const allocator = self.arena.allocator();
            self.watch_heads = try allocator.alloc(usize, watcher.?.slots.items.len);
            @memset(self.watch_heads, std.math.maxInt(usize));
            for (self.entries.items, 0..) |entry, i| {
                if (entry.blob.token) |token| self.linkWatch(i, token.slot);
            }
        }
        for (self.entries.items, 0..) |entry, i| {
            try self.ensureDirectory(std.fs.path.dirname(entry.path) orelse "");
            self.linkDirectory(i);
        }
        self.refresh_entries_visited = self.entries.items.len;
        if (previous) |old| {
            self.parent_revision = old.revision;
            if (std.mem.eql(u8, &self.revision, &old.revision)) {
                self.parent_revision = old.parent_revision;
                for (old.changes.items) |path| try self.addChange(path);
            }
            var left_it = old.iterator();
            var right_it = self.iterator();
            var left = left_it.next();
            var right = right_it.next();
            while (left != null or right != null) {
                const order: std.math.Order = if (left == null) .gt else if (right == null) .lt else std.mem.order(u8, left.?.path, right.?.path);
                const path = if (order == .lt) left.?.path else right.?.path;
                if (order != .eq or !std.mem.eql(u8, &left.?.digest, &right.?.digest) or left.?.explicit_file != right.?.explicit_file)
                    try self.addChange(path);
                if (order != .gt) left = left_it.next();
                if (order != .lt) right = right_it.next();
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
        const revision = entry.revision();
        h.update(&revision);
        h.update(&.{@intFromBool(entry.explicit_file)});
        var digest: [32]u8 = undefined;
        h.final(&digest);
        return digest;
    }
    fn rootRevision(count: usize, root: [32]u8) [64]u8 {
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update("antfly-workspace-root-v3");
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, count, .little);
        h.update(&length);
        h.update(&root);
        var digest: [32]u8 = undefined;
        h.final(&digest);
        return std.fmt.bytesToHex(digest, .lower);
    }
    fn inventoryRevision(root: ?*inventory.Node) [64]u8 {
        return rootRevision(if (root) |n| n.count else 0, if (root) |n| n.hash else @splat(0));
    }
    fn ensureDirectory(self: *Generation, path: []const u8) !void {
        if (self.directory_heads.contains(path)) return;
        const key = try self.arena.allocator().dupe(u8, path);
        try self.directory_heads.put(self.budget.allocator(), key, std.math.maxInt(usize));
    }
    fn linkDirectory(self: *Generation, i: usize) void {
        const entry = &self.entries.items[i];
        const head = self.directory_heads.getPtr(std.fs.path.dirname(entry.path) orelse "").?;
        entry.directory_prev = std.math.maxInt(usize);
        entry.directory_next = head.*;
        if (head.* != std.math.maxInt(usize)) self.entries.items[head.*].directory_prev = i;
        head.* = i;
    }
    fn unlinkDirectory(self: *Generation, i: usize) void {
        const entry = self.entries.items[i];
        if (entry.directory_prev == std.math.maxInt(usize)) self.directory_heads.getPtr(std.fs.path.dirname(entry.path) orelse "").?.* = entry.directory_next else self.entries.items[entry.directory_prev].directory_next = entry.directory_next;
        if (entry.directory_next != std.math.maxInt(usize)) self.entries.items[entry.directory_next].directory_prev = entry.directory_prev;
    }
    fn ensureWatchHeads(self: *Generation, capacity: usize) !void {
        if (capacity <= self.watch_heads.len) return;
        const heads = try self.arena.allocator().alloc(usize, capacity);
        @memcpy(heads[0..self.watch_heads.len], self.watch_heads);
        @memset(heads[self.watch_heads.len..], std.math.maxInt(usize));
        self.watch_heads = heads;
    }
    fn linkWatch(self: *Generation, i: usize, slot: usize) void {
        const prior = self.watch_heads[slot];
        self.entries.items[i].watch_next = prior;
        self.entries.items[i].watch_prev = std.math.maxInt(usize);
        if (prior != std.math.maxInt(usize)) self.entries.items[prior].watch_prev = i;
        self.watch_heads[slot] = i;
        self.entries.items[i].watch_slot = slot;
    }
    fn unlinkWatch(self: *Generation, i: usize) void {
        const entry = &self.entries.items[i];
        if (entry.watch_slot == std.math.maxInt(usize)) return;
        if (entry.watch_prev == std.math.maxInt(usize)) self.watch_heads[entry.watch_slot] = entry.watch_next else self.entries.items[entry.watch_prev].watch_next = entry.watch_next;
        if (entry.watch_next != std.math.maxInt(usize)) self.entries.items[entry.watch_next].watch_prev = entry.watch_prev;
        entry.watch_slot = std.math.maxInt(usize);
    }
    /// Query workers have joined and the namespace certificate is unchanged.
    /// Stage dirty contents and Merkle paths before touching the published tree.
    /// Exclusive ownership permits transactional replacement without retaining
    /// a chain of prior generations or copying the unchanged inventory.
    fn captureDirty(a: A, io: std.Io, o: *const Options, old: *Generation, w: *Watch, journal: *@import("discovery_journal.zig").Journal) !?*Generation {
        if (old.watch_heads.len == 0 or old.touch_epoch == std.math.maxInt(u64)) return null;
        const epoch = w.epoch;
        const native_epoch = journal.native.checkpoint();
        const native_refresh = journal.native_refresh;
        const original_limit = old.budget.limit;
        var published = false;
        defer if (!published) {
            old.budget.limit = original_limit;
        };
        old.touch_epoch += 1;
        var stage: Generation = .{ .backing = a, .budget = .{ .backing = a, .limit = o.workspace_memory_limit - @sizeOf(Generation) }, .arena = undefined };
        stage.arena = .init(stage.budget.allocator());
        defer {
            for (stage.entries.items) |entry| {
                entry.release();
                stage.budget.allocator().free(entry.path);
            }
            stage.entries.deinit(stage.budget.allocator());
            stage.arena.deinit();
        }
        var indices: std.ArrayList(usize) = .empty;
        var tree = inventory.retain(old.tree);
        defer inventory.release(old.budget.allocator(), tree);
        const scratch = stage.arena.allocator();
        // Freeze the event suffix; any new event during capture abandons the
        // optimization before publication and uses conservative capture.
        const event_count = w.event_count;
        for (w.events.items[0..event_count]) |slot| {
            // A failed staged refresh may have admitted later watch slots;
            // those have no routes in the still-published generation.
            if (slot >= old.watch_heads.len) continue;
            var i = old.watch_heads[slot];
            while (i != std.math.maxInt(usize)) : (i = old.entries.items[i].watch_next) {
                if (old.entries.items[i].touched == old.touch_epoch) continue;
                old.entries.items[i].touched = old.touch_epoch;
                try indices.append(scratch, i);
            }
        }
        if (native_refresh) {
            journal.native.lock();
            defer journal.native.unlock();
            var paths = journal.native.paths.keyIterator();
            while (paths.next()) |path| if (journal.sourceIndex(path.*)) |i| {
                const entry = old.entries.items[i];
                // An advisory/delayed event may describe a file already read.
                // A strict intact inode witness proves its payload is current.
                // The final source epoch barrier catches racing new writes.
                if (entry.blob.token) |token| if (w.intact(token)) continue;
                if (old.entries.items[i].touched == old.touch_epoch) continue;
                old.entries.items[i].touched = old.touch_epoch;
                try indices.append(scratch, i);
            };
        }
        if (indices.items.len == 0) return null;
        var collector: Collector = .{ .generation = &stage, .io = io, .options = o, .previous = old, .watcher = w };
        var retained = old.blob_bytes;
        var source_bytes = old.source_bytes;
        var changes: std.ArrayList([]const u8) = .empty;
        defer freeChanges(old.budget.allocator(), &changes);
        for (indices.items) |i| {
            const entry = old.entries.items[i];
            const before = stage.entries.items.len;
            try collector.addFile(entry.path, entry.path, entry.explicit_file);
            // Oversize exclusion changes membership, requiring ordinary capture.
            if (stage.entries.items.len != before + 1) return null;
            const next = stage.entries.items[before];
            source_bytes = source_bytes - entry.bytes().len + next.bytes().len;
            retained = retained - entry.charge() + next.charge();
            if (!std.mem.eql(u8, &entry.digest, &next.digest)) try appendChange(old.budget.allocator(), &changes, entry.path);
            const updated = try inventory.put(old.budget.allocator(), tree, entry.path, i, leafHash(next));
            inventory.release(old.budget.allocator(), tree);
            tree = updated;
        }
        try old.ensureWatchHeads(w.slots.items.len);
        if (retained + old.budget.live + @sizeOf(Generation) > o.workspace_memory_limit) return null;
        const revision = inventoryRevision(tree);
        w.poll();
        if (!journal.clean() or w.failed or w.epoch != epoch) {
            if (o.trace) std.debug.print("antfly-grep: dirty refresh retry source_epoch={d}/{d} reason={s}\n", .{ w.epoch, epoch, journal.rejection });
            return null;
        }
        if (native_refresh) {
            if (!journal.namespaceClean()) return null;
            journal.native.lock();
            defer journal.native.unlock();
            if (journal.native.epoch != native_epoch) {
                if (o.trace) std.debug.print("antfly-grep: dirty refresh retry directory_epoch={d}/{d}\n", .{ journal.native.epoch, native_epoch });
                return null;
            }
        } else for (stage.entries.items) |entry| if (entry.blob.token == null or !w.intact(entry.blob.token.?)) return null;
        var missing_inode_witness = false;
        var requires_inode_witness = false;
        for (stage.entries.items) |entry| if (entry.blob.token == null or !w.intact(entry.blob.token.?)) {
            missing_inode_witness = true;
            if (entry.explicit_file or (entry.blob.stamp.valid and entry.blob.stamp.nlink > 1)) requires_inode_witness = true;
        };
        const native_source_proof = @import("builtin").os.tag == .linux and journal.allow_native_sources and journal.native_covered and !requires_inode_witness;
        const sources_covered = journal.sources_covered and (!missing_inode_witness or native_source_proof);
        // No fallible work remains after this point.
        const content_changed = !std.mem.eql(u8, &revision, &old.revision);
        if (content_changed) {
            old.parent_revision = old.revision;
            freeChanges(old.budget.allocator(), &old.changes);
            old.changes = changes;
            changes = .empty;
        }
        old.preparations_built = 0;
        old.preparations_reused = old.preparation_count;
        for (indices.items, stage.entries.items) |i, next| {
            const prior = old.entries.items[i];
            old.unlinkWatch(i);
            old.unlinkDirectory(i);
            old.entries.items[i] = next;
            old.entries.items[i].path = prior.path;
            next.retain();
            old.preparation_count = old.preparation_count - @intFromBool(prior.preparation != null) + @intFromBool(next.preparation != null);
            prior.release();
            if (next.blob.token) |token| old.linkWatch(i, token.slot);
            old.linkDirectory(i);
        }
        inventory.release(old.budget.allocator(), old.tree);
        old.tree = tree;
        tree = null;
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
        journal.sources_covered = sources_covered;
        if (requires_inode_witness) journal.allow_native_sources = false;
        journal.native_active = journal.native_active or missing_inode_witness;
        old.watch_fallback = !sources_covered or !journal.coverage;
        if (native_refresh) {
            journal.rearmDirectories();
            journal.native.retire(native_epoch);
            journal.native_epoch = native_epoch;
        }
        return old;
    }
    /// Reconcile only file membership in certified existing directories. A
    /// topology/configuration change, lost witness, or race uses full discovery.
    /// All fallible work precedes publication, including slot moves and hashes.
    fn captureMembership(a: A, io: std.Io, o: *const Options, old: *Generation, w: *Watch, j: *@import("discovery_journal.zig").Journal) !?*Generation {
        if (!j.sealed or !j.coverage or old.watch_heads.len == 0) return null;
        j.membership_dirs.clearRetainingCapacity();
        j.allow_membership = true;
        defer j.allow_membership = false;
        if (!j.namespaceClean() or j.membership_dirs.items.len == 0) return null;
        w.poll();
        const epoch = w.epoch;
        const namespace_epoch = j.watch.epoch;
        const native_epoch = j.native.checkpoint();
        var stage: Generation = .{ .backing = a, .budget = .{ .backing = a, .limit = o.workspace_memory_limit - @sizeOf(Generation) }, .arena = undefined };
        stage.arena = .init(stage.budget.allocator());
        defer {
            for (stage.entries.items) |entry| {
                entry.release();
                stage.budget.allocator().free(entry.path);
            }
            stage.entries.deinit(stage.budget.allocator());
            stage.arena.deinit();
        }
        const scratch = stage.arena.allocator();
        var seen: std.AutoHashMapUnmanaged(usize, void) = .empty;
        var eligibility: std.ArrayList([]const u8) = .empty;
        var collector: Collector = .{ .generation = &stage, .io = io, .options = o, .previous = old, .watcher = w, .membership_seen = &seen, .eligibility = &eligibility };
        var considered: std.ArrayList([]const u8) = .empty;
        var scanned_dirs: std.StringHashMapUnmanaged(void) = .empty;
        for (j.membership_dirs.items) |directory| {
            try scanned_dirs.put(scratch, directory.display, {});
            var root: ?@import("discovery_journal.zig").Journal.Root = null;
            for (j.roots.items) |candidate| {
                if (std.mem.eql(u8, candidate.path, directory.path) or (std.mem.startsWith(u8, directory.path, candidate.path) and directory.path.len > candidate.path.len and (std.mem.endsWith(u8, candidate.path, "/") or directory.path[candidate.path.len] == '/'))) {
                    if (root == null or candidate.path.len > root.?.path.len) root = candidate;
                }
            }
            const scope = root orelse return null;
            // Overlapping roots already produce duplicate paths in cold capture.
            // Match the original display prefix before using this policy chain.
            const suffix = directory.path[scope.path.len..];
            const expected = if (scope.display.len == 0) std.mem.trimStart(u8, suffix, "/") else try std.mem.concat(scratch, u8, &.{ scope.display, suffix });
            if (!std.mem.eql(u8, expected, directory.display)) return null;
            var discovery = @import("discovery.zig").Discovery{ .a = scratch, .io = io, .options = o, .shallow = true };
            defer discovery.deinit();
            try discovery.prepareWithin(scope.path, directory.path);
            if (directory.native_names) {
                try discovery.prepareDirectoryRules(directory.path);
                var paths: std.ArrayList([]const u8) = .empty;
                j.native.lock();
                for (j.native.pathsInDirectory(directory.path)) |name| {
                    const owned = scratch.dupe(u8, name) catch {
                        j.native.unlock();
                        return null;
                    };
                    paths.append(scratch, owned) catch {
                        j.native.unlock();
                        return null;
                    };
                }
                j.native.unlock();
                for (paths.items) |full| {
                    const shown = if (directory.display.len == 0) try scratch.dupe(u8, std.fs.path.basename(full)) else try std.fs.path.join(scratch, &.{ directory.display, std.fs.path.basename(full) });
                    try considered.append(scratch, shown);
                    const stat = std.Io.Dir.cwd().statFile(io, full, .{ .follow_symlinks = false }) catch |err| {
                        if (err == error.FileNotFound or err == error.NotDir) continue;
                        return null;
                    };
                    if (stat.kind == .file and try discovery.fileAllowed(full, shown)) try collector.addFile(full, shown, false);
                }
            } else try discovery.walk(directory.path, directory.display, &collector);
        }
        var staged: std.StringHashMapUnmanaged(void) = .empty;
        for (stage.entries.items) |entry| try staged.put(scratch, entry.path, {});
        var removed: std.ArrayList([]const u8) = .empty;
        for (considered.items) |path| if (old.find(path)) |entry| {
            const slot = (@intFromPtr(entry) - @intFromPtr(old.entries.items.ptr)) / @sizeOf(Entry);
            if (!seen.contains(slot)) try removed.append(scratch, entry.path);
        };
        for (j.membership_dirs.items) |directory| {
            if (directory.native_names) continue;
            var slot = old.directory_heads.get(directory.display) orelse std.math.maxInt(usize);
            while (slot != std.math.maxInt(usize)) {
                const entry = old.entries.items[slot];
                if (!seen.contains(slot)) try removed.append(scratch, entry.path);
                slot = entry.directory_next;
            }
        }
        // Include writes outside the directories whose names changed. A single
        // refresh may combine an add/delete with arbitrary source edits.
        const event_count = w.event_count;
        for (w.events.items[0..event_count]) |watch_slot| {
            if (watch_slot >= old.watch_heads.len) continue;
            var slot = old.watch_heads[watch_slot];
            while (slot != std.math.maxInt(usize)) : (slot = old.entries.items[slot].watch_next) {
                const entry = old.entries.items[slot];
                const scanned = scanned_dirs.contains(std.fs.path.dirname(entry.path) orelse "");
                if (!scanned and !staged.contains(entry.path)) {
                    const before = stage.entries.items.len;
                    try collector.addFile(entry.path, entry.path, entry.explicit_file);
                    if (stage.entries.items.len != before + 1 and !seen.contains(slot)) return null;
                    try staged.put(scratch, entry.path, {});
                }
            }
        }
        // Linux directory-only witnesses can describe source writes outside the
        // changed membership directories as well.
        j.native.lock();
        const native_stable = j.native.epoch == native_epoch;
        var native_paths: std.ArrayList([]const u8) = .empty;
        var events = j.native.paths.keyIterator();
        while (events.next()) |path| if (j.sourceIndex(path.*)) |slot| {
            const entry = old.entries.items[slot];
            const scanned = scanned_dirs.contains(std.fs.path.dirname(entry.path) orelse "");
            if (!scanned and !staged.contains(entry.path)) {
                native_paths.append(scratch, entry.path) catch {
                    j.native.unlock();
                    return null;
                };
                staged.put(scratch, entry.path, {}) catch {
                    j.native.unlock();
                    return null;
                };
            }
        };
        j.native.unlock();
        if (!native_stable) return null;
        for (native_paths.items) |path| {
            const before = stage.entries.items.len;
            try collector.addFile(path, path, old.find(path).?.explicit_file);
            if (stage.entries.items.len != before + 1 and !seen.contains(@as(usize, (@intFromPtr(old.find(path).?) - @intFromPtr(old.entries.items.ptr)) / @sizeOf(Entry)))) return null;
        }
        var tree = inventory.retain(old.tree);
        defer inventory.release(old.budget.allocator(), tree);
        var changes: std.ArrayList([]const u8) = .empty;
        defer freeChanges(old.budget.allocator(), &changes);
        const Remove = struct { slot: usize, last: usize };
        var removes: std.ArrayList(Remove) = .empty;
        var moved: std.AutoHashMapUnmanaged(usize, Entry) = .empty;
        var count = old.entries.items.len;
        var retained = old.blob_bytes;
        var bytes = old.source_bytes;
        var preparations = old.preparation_count;
        for (removed.items) |path| {
            const slot = inventory.find(tree, path) orelse return null;
            const entry = moved.get(slot) orelse old.entries.items[slot];
            const next = try inventory.remove(old.budget.allocator(), tree, path);
            inventory.release(old.budget.allocator(), tree);
            tree = next;
            try appendChange(old.budget.allocator(), &changes, path);
            retained -= entry.charge();
            bytes -= entry.bytes().len;
            preparations -= @intFromBool(entry.preparation != null);
            count -= 1;
            if (slot != count) {
                const last = moved.get(count) orelse old.entries.items[count];
                const updated = try inventory.put(old.budget.allocator(), tree, last.path, slot, leafHash(last));
                inventory.release(old.budget.allocator(), tree);
                tree = updated;
                try moved.put(scratch, slot, last);
            }
            _ = moved.remove(count);
            try removes.append(scratch, .{ .slot = slot, .last = count });
        }
        const Upsert = struct { slot: usize, added: bool, path: ?[]const u8 = null, skip: bool = false };
        var upserts: std.ArrayList(Upsert) = .empty;
        defer for (upserts.items) |op| if (op.path) |path| old.budget.allocator().free(path);
        for (stage.entries.items) |entry| {
            const prior_slot = inventory.find(tree, entry.path);
            const slot = prior_slot orelse count;
            if (prior_slot) |_| {
                const prior = moved.get(slot) orelse old.entries.items[slot];
                if (prior.blob == entry.blob and prior.preparation == entry.preparation and prior.explicit_file == entry.explicit_file and prior.watch_slot == (if (entry.blob.token) |token| token.slot else std.math.maxInt(usize))) {
                    try upserts.append(scratch, .{ .slot = slot, .added = false, .skip = true });
                    continue;
                }
                retained -= prior.charge();
                bytes -= prior.bytes().len;
                preparations -= @intFromBool(prior.preparation != null);
                if (!std.mem.eql(u8, &prior.digest, &entry.digest) or prior.explicit_file != entry.explicit_file) try appendChange(old.budget.allocator(), &changes, entry.path);
            } else {
                if (count >= o.workspace_max_files) return null;
                count += 1;
                try appendChange(old.budget.allocator(), &changes, entry.path);
            }
            retained += entry.charge();
            bytes += entry.bytes().len;
            preparations += @intFromBool(entry.preparation != null);
            const next = try inventory.put(old.budget.allocator(), tree, entry.path, slot, leafHash(entry));
            inventory.release(old.budget.allocator(), tree);
            tree = next;
            try upserts.append(scratch, .{ .slot = slot, .added = prior_slot == null });
            if (prior_slot == null) upserts.items[upserts.items.len - 1].path = try old.budget.allocator().dupe(u8, entry.path);
            try old.ensureDirectory(std.fs.path.dirname(entry.path) orelse "");
        }
        // New size-excluded files still affect future eligibility. Arm their
        // dependency before confirming the exclusion; do not certify an event
        // which raced subscription/validation.
        for (eligibility.items) |path| {
            j.dependency(path);
            if (!j.coverage) return null;
            const token = j.tokens.get(path) orelse return null;
            const stat = try std.Io.Dir.cwd().statFile(io, path, .{});
            if (stat.size <= o.max_file_bytes) return null;
            j.watch.certify(token);
            if (!j.watch.intact(token)) return null;
        }
        try old.entries.ensureTotalCapacity(old.budget.allocator(), count);
        try old.ensureWatchHeads(w.slots.items.len);
        if (retained + old.budget.live + @sizeOf(Generation) > o.workspace_memory_limit) return null;
        w.poll();
        j.watch.poll();
        if (w.failed or w.epoch != epoch or j.watch.epoch != namespace_epoch or !j.namespaceClean()) return null;
        if (j.native.checkpoint() != native_epoch) return null;
        for (stage.entries.items) |entry| if (entry.witness_reused and (entry.blob.token == null or !w.intact(entry.blob.token.?))) return null;
        const revision = inventoryRevision(tree);
        const changed = !std.mem.eql(u8, &revision, &old.revision);
        // Publication below cannot allocate, fail, or invoke external code.
        if (changed) {
            old.parent_revision = old.revision;
            freeChanges(old.budget.allocator(), &old.changes);
            old.changes = changes;
            changes = .empty;
        }
        for (removes.items) |op| {
            old.unlinkWatch(op.slot);
            old.unlinkDirectory(op.slot);
            if (op.slot != op.last) {
                old.unlinkWatch(op.last);
                old.unlinkDirectory(op.last);
            }
            const entry = old.entries.swapRemove(op.slot);
            entry.release();
            old.budget.allocator().free(entry.path);
            old.path_bytes -= entry.path.len;
            if (op.slot != op.last) {
                old.linkDirectory(op.slot);
                if (old.entries.items[op.slot].blob.token) |token| old.linkWatch(op.slot, token.slot);
            }
        }
        for (stage.entries.items, upserts.items) |entry, *op| {
            if (op.skip) continue;
            var next = entry;
            if (op.added) {
                next.path = op.path.?;
                old.path_bytes += next.path.len;
                op.path = null;
                old.entries.appendAssumeCapacity(next);
            } else {
                const prior = old.entries.items[op.slot];
                old.unlinkWatch(op.slot);
                old.unlinkDirectory(op.slot);
                next.path = prior.path;
                old.entries.items[op.slot] = next;
                prior.release();
            }
            next.retain();
            old.linkDirectory(op.slot);
            if (next.blob.token) |token| old.linkWatch(op.slot, token.slot);
        }
        inventory.release(old.budget.allocator(), old.tree);
        old.tree = tree;
        tree = null;
        old.revision = revision;
        old.blob_bytes = retained;
        old.budget.limit = o.workspace_memory_limit - @sizeOf(Generation) - retained;
        old.source_bytes = bytes;
        old.preparation_count = preparations;
        old.preparations_reused = preparations;
        old.preparations_built = 0;
        old.sources_opened = stage.sources_opened;
        old.source_bytes_read = stage.source_bytes_read;
        old.source_bytes_reused = bytes -| stage.source_bytes_read;
        old.refresh_entries_visited = collector.membership_visited + removed.items.len;
        old.captured_at = std.Io.Clock.real.now(io).toSeconds();
        old.discovery_reused = true;
        for (j.membership_dirs.items) |directory| j.inventories.getPtr(directory.path).?.* = if (directory.native_names) null else directory.stamp;
        for (stage.entries.items) |entry| if (entry.blob.token == null or !w.intact(entry.blob.token.?)) {
            if (@import("builtin").os.tag != .linux or entry.explicit_file or (entry.blob.stamp.valid and entry.blob.stamp.nlink > 1) or !j.native_covered or !j.allow_native_sources) j.sources_covered = false;
            j.native_active = true;
        };
        j.source_epoch = w.epoch;
        w.clearEvents();
        j.rearmDirectories();
        j.native.retire(native_epoch);
        j.native_epoch = native_epoch;
        old.watch_fallback = !j.sources_covered or !j.coverage;
        old.refs += 1;
        return old;
    }
    /// Immutable content has an optional, independently owned derived memo.
    /// Reserve its peak before parsing, then publish only a complete preparation.
    /// Workers prepare different entries concurrently; metadata admission alone
    /// takes the generation mutex. Failure is an ordinary uncached-query fallback.
    pub fn prepareEntry(generation: *const Generation, io: std.Io, entry: *const Entry) bool {
        var state: source.State = .{};
        return generation.prepareEntryCached(io, entry, &.{ .no_cache = true }, null, &state) != .none;
    }
    pub const Preparation = enum { none, parsed, cached };
    pub fn prepareEntryCached(generation: *const Generation, io: std.Io, entry: *const Entry, options: *const Options, scope: ?[32]u8, state: *source.State) Preparation {
        const self = @constCast(generation);
        if (options.crlf or options.null_data or entry.preparation != null or !std.unicode.utf8ValidateSlice(entry.bytes()) or std.mem.indexOfScalar(u8, entry.bytes(), 0) != null or @import("antfly_encoding").hasBom(entry.bytes())) return .none;
        self.preparation_mutex.lockUncancelable(io);
        const available = self.budget.limit -| self.budget.live;
        const desired = @max(2 * 1024 * 1024, @min(32 * 1024 * 1024, std.math.mul(usize, entry.bytes().len, 16) catch std.math.maxInt(usize)));
        const reservation = @min(available, desired);
        if (reservation <= @sizeOf(PreparedOwner)) {
            self.preparation_mutex.unlock(io);
            return .none;
        }
        self.budget.limit -= reservation;
        self.preparation_mutex.unlock(io);
        var hit = false;
        const owner = PreparedOwner.create(self.backing, reservation, io, entry.path, entry.bytes(), entry.digest, options, scope, state, &hit) catch {
            self.preparation_mutex.lockUncancelable(io);
            self.budget.limit += reservation;
            self.preparation_mutex.unlock(io);
            return .none;
        };
        self.preparation_mutex.lockUncancelable(io);
        defer self.preparation_mutex.unlock(io);
        const size = owner.bytes();
        const mutable = @constCast(entry);
        mutable.preparation = owner;
        self.budget.limit += reservation - size;
        self.blob_bytes += size;
        self.preparation_count += 1;
        self.preparations_built += 1;
        return if (hit) .cached else .parsed;
    }
    /// Shared blobs count in full against every generation that retains them.
    pub fn retainedBytes(self: *const Generation) usize {
        return self.budget.live + @sizeOf(Generation) + self.blob_bytes;
    }
    pub fn deinit(self: *Generation) void {
        self.refs -= 1;
        if (self.refs != 0) return;
        const a = self.backing;
        for (self.entries.items) |entry| {
            entry.release();
            self.budget.allocator().free(entry.path);
        }
        self.entries.deinit(self.budget.allocator());
        self.directory_heads.deinit(self.budget.allocator());
        freeChanges(self.budget.allocator(), &self.changes);
        inventory.release(self.budget.allocator(), self.tree);
        self.arena.deinit();
        a.destroy(self);
    }
    fn appendChange(a: A, list: *std.ArrayList([]const u8), path: []const u8) !void {
        const owned = try a.dupe(u8, path);
        errdefer a.free(owned);
        try list.append(a, owned);
    }
    fn addChange(self: *Generation, path: []const u8) !void {
        try appendChange(self.budget.allocator(), &self.changes, path);
    }
    fn freeChanges(a: A, list: *std.ArrayList([]const u8)) void {
        for (list.items) |path| a.free(path);
        list.deinit(a);
        list.* = .empty;
    }
    pub fn find(self: *const Generation, path_name: []const u8) ?*const Entry {
        const slot = inventory.find(self.tree, path_name) orelse return null;
        return &self.entries.items[slot];
    }
    pub const Iterator = struct {
        generation: *const Generation,
        tree: inventory.Iterator,
        pub fn next(self: *Iterator) ?*const Entry {
            const slot = self.tree.next() orelse return null;
            return &self.generation.entries.items[slot];
        }
    };
    pub fn iterator(self: *const Generation) Iterator {
        return .{ .generation = self, .tree = .init(self.tree, null) };
    }
};
/// Independent allocations never borrow a generation allocator. Refresh can
/// release the old generation immediately, including after arbitrarily many reuses.
const Stamp = struct {
    inode: @FieldType(std.Io.File.Stat, "inode") = undefined,
    size: u64 = undefined,
    mtime: @FieldType(std.Io.File.Stat, "mtime") = undefined,
    ctime: @FieldType(std.Io.File.Stat, "ctime") = undefined,
    nlink: @FieldType(std.Io.File.Stat, "nlink") = undefined,
    kind: @FieldType(std.Io.File.Stat, "kind") = .unknown,
    valid: bool = false,
    fn from(value: std.Io.File.Stat) Stamp {
        return .{ .inode = value.inode, .size = value.size, .mtime = value.mtime, .ctime = value.ctime, .nlink = value.nlink, .kind = value.kind, .valid = true };
    }
};
const Blob = struct {
    backing: A,
    refs: usize = 1, // capture/publication run serially after query workers join
    bytes: []u8,
    stamp: Stamp = .{},
    watcher: ?*Watch = null,
    token: ?Token = null,
    fn create(a: A, size: usize) !*Blob {
        const storage = try a.alignedAlloc(u8, .of(Blob), try std.math.add(usize, @sizeOf(Blob), size));
        const self: *Blob = @ptrCast(storage.ptr);
        self.* = .{ .backing = a, .bytes = storage[@sizeOf(Blob)..] };
        return self;
    }
    fn charge(self: *const Blob) usize {
        return @sizeOf(Blob) + self.bytes.len;
    }
    fn release(self: *Blob) void {
        self.refs -= 1;
        if (self.refs != 0) return;
        if (self.watcher) |watcher| if (self.token) |token| watcher.release(token);
        const storage: [*]align(@alignOf(Blob)) u8 = @ptrCast(self);
        self.backing.free(storage[0..self.charge()]);
    }
};
const PreparedOwner = struct {
    backing: A,
    refs: usize = 1,
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    value: source.Prepared,
    fn create(a: A, maximum: usize, io: std.Io, path: []const u8, bytes_value: []const u8, revision: [32]u8, options: *const Options, scope: ?[32]u8, state: *source.State, hit: *bool) !*PreparedOwner {
        const self = try a.create(PreparedOwner);
        errdefer a.destroy(self);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = maximum - @sizeOf(PreparedOwner) }, .arena = undefined, .value = undefined };
        self.arena = .init(self.budget.allocator());
        errdefer self.arena.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const value = try source.getHashed(temporary, io, options, path, bytes_value, scope, hit, state, revision);
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
fn readBlob(io: std.Io, file: std.Io.File, before: anytype, blob: *Blob) ![32]u8 {
    // The before/after stamp includes size and ctime. Reading directly into the
    // immutable owner avoids a small-file buffer copy and a separate EOF probe;
    // a growing file is rejected by the final stamp, a shrinking file by the
    // short read. No shared file position or proportional scratch is needed.
    if (try file.readPositionalAll(io, blob.bytes, 0) != blob.bytes.len) return error.WorkspaceChangedDuringCapture;
    const after = try file.stat(io);
    if (!Collector.sameStamp(before, after)) return error.WorkspaceChangedDuringCapture;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(blob.bytes, &digest, .{});
    return digest;
}

test "direct snapshot reads reject growth and shrinkage and preserve empty file hashes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "input", .data = "old" });
    const file = try tmp.dir.openFile(io, "input", .{});
    defer file.close(io);
    const before = try file.stat(io);
    const blob = try Blob.create(a, 3);
    defer blob.release();
    _ = try readBlob(io, file, before, blob);
    try std.testing.expectEqualStrings("old", blob.bytes);
    try tmp.dir.writeFile(io, .{ .sub_path = "input", .data = "longer" });
    try std.testing.expectError(error.WorkspaceChangedDuringCapture, readBlob(io, file, before, blob));
    try tmp.dir.writeFile(io, .{ .sub_path = "input", .data = "x" });
    try std.testing.expectError(error.WorkspaceChangedDuringCapture, readBlob(io, file, before, blob));
    try tmp.dir.writeFile(io, .{ .sub_path = "input", .data = "" });
    const empty = try Blob.create(a, 0);
    defer empty.release();
    const hash = try readBlob(io, file, try file.stat(io), empty);
    var expected: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("", &expected, .{});
    try std.testing.expectEqualSlices(u8, &expected, &hash);
}

// Cold opens overlap filesystem metadata latency. Workers own disjoint job
// slots; only the coordinator allocates source blobs, arms subscriptions and
// publishes entries. Both queues share the descriptor allowance. Paths are
// owned because discovery reuses its scratch buffer between callbacks.
const OpenQueue = struct {
    const Job = struct {
        path: []u8,
        display: []const u8,
        file: ?std.Io.File = null,
        stamp: std.Io.File.Stat = undefined,
        explicit_file: bool,
        failure: ?anyerror = null,
    };
    collector: *Collector,
    pool: *@import("worker_pool.zig").Pool,
    workers: usize,
    capacity: usize,
    jobs: [16]Job = undefined,
    count: usize = 0,
    cursor: std.atomic.Value(usize) = .init(0),
    fn push(self: *OpenQueue, path: []const u8, display: []const u8, explicit_file: bool) !void {
        const a = self.collector.generation.budget.allocator();
        const same = std.mem.eql(u8, path, display);
        const storage = try a.alloc(u8, path.len + if (same) @as(usize, 0) else display.len);
        @memcpy(storage[0..path.len], path);
        if (!same) @memcpy(storage[path.len..], display);
        self.jobs[self.count] = .{ .path = storage, .display = if (same) storage else storage[path.len..], .explicit_file = explicit_file };
        // Keep path length separate from the shared allocation through a slice.
        self.jobs[self.count].path = storage[0..path.len];
        self.count += 1;
    }
    fn clear(self: *OpenQueue) void {
        for (self.jobs[0..self.count]) |job| {
            if (job.file) |file| file.close(self.collector.io);
            const len = job.path.len + if (job.display.ptr == job.path.ptr) @as(usize, 0) else job.display.len;
            self.collector.generation.budget.allocator().free(job.path.ptr[0..len]);
        }
        self.count = 0;
    }
    fn run(self: *OpenQueue) void {
        while (true) {
            const index = self.cursor.fetchAdd(1, .monotonic);
            if (index >= self.count) return;
            const job = &self.jobs[index];
            const file = std.Io.Dir.cwd().openFile(self.collector.io, job.path, .{}) catch |err| {
                job.failure = err;
                continue;
            };
            job.file = file;
            job.stamp = file.stat(self.collector.io) catch |err| {
                job.failure = err;
                continue;
            };
        }
    }
    fn flush(self: *OpenQueue) !void {
        if (self.count == 0) return;
        defer self.clear();
        self.cursor.store(0, .monotonic);
        var handles: [8]@import("worker_pool.zig").Handle = undefined;
        var launched: usize = 0;
        if (self.count > 1) for (0..@min(self.workers, self.count) - 1) |_| {
            handles[launched] = self.pool.launch(run, self) catch break;
            launched += 1;
        };
        self.run();
        for (handles[0..launched]) |handle| handle.join();
        if (launched > 0) {
            self.collector.generation.capture_workers = @max(self.collector.generation.capture_workers, launched + 1);
            self.collector.generation.parallel_sources_opened += self.count;
        }
        const serial_retry = for (self.jobs[0..self.count]) |job| {
            if (job.failure != null and job.file == null) break true;
        } else false;
        if (serial_retry) {
            // An optional parallel open must not make capture fail solely
            // because other successful opens hold scarce descriptors. Retire
            // the entire open batch, then admit one file at a time.
            for (self.jobs[0..self.count]) |*job| if (job.file) |file| {
                file.close(self.collector.io);
                job.file = null;
            };
            if (self.collector.queue) |queue| try queue.flush();
        }
        for (self.jobs[0..self.count]) |*job| {
            if (serial_retry) {
                job.file = std.Io.Dir.cwd().openFile(self.collector.io, job.path, .{}) catch |err| blk: {
                    const queue = self.collector.queue orelse return err;
                    if (queue.count == 0) return err;
                    try queue.flush();
                    break :blk try std.Io.Dir.cwd().openFile(self.collector.io, job.path, .{});
                };
                job.stamp = try job.file.?.stat(self.collector.io);
            } else if (job.failure) |err| return err;
            const file = job.file.?;
            job.file = null;
            try self.collector.addOpened(file, job.stamp, job.path, job.display, job.explicit_file);
        }
    }
    fn deinit(self: *OpenQueue) void {
        self.clear();
    }
};

// Cold capture batches own a fixed number of descriptors and at most 8 MiB
// of source payloads. Allocation, subscriptions and publication stay serial;
// workers only read, validate and hash disjoint preallocated immutable blobs.
// Auto mode keeps tiny files serial, where coordination can cost more than I/O.
const CaptureQueue = struct {
    const Job = struct {
        file: std.Io.File,
        blob: ?*Blob,
        display: []u8,
        explicit_file: bool,
        reserved: usize,
        revision: [32]u8 = undefined,
        failure: ?anyerror = null,
    };
    const byte_limit = 8 * 1024 * 1024;
    collector: *Collector,
    pool: @import("worker_pool.zig").Pool,
    workers: usize,
    capacity: usize = 32,
    jobs: [32]Job = undefined,
    count: usize = 0,
    bytes: usize = 0,
    cursor: std.atomic.Value(usize) = .init(0),
    fn descriptorCapacity() usize {
        if (comptime @import("builtin").link_libc and (@import("builtin").os.tag == .macos or @import("builtin").os.tag == .linux)) {
            var quota: std.c.rlimit = undefined;
            if (std.c.getrlimit(.NOFILE, &quota) == 0) return @max(1, @min(32, @as(usize, @intCast(quota.cur / 8))));
        }
        return 32;
    }
    fn push(self: *CaptureQueue, file: std.Io.File, blob: *Blob, display: []const u8, explicit_file: bool) !void {
        const generation = self.collector.generation;
        const a = generation.budget.allocator();
        const name = try a.dupe(u8, display);
        errdefer a.free(name);
        const size = blob.charge();
        if (size > generation.budget.limit -| generation.budget.live) return error.OutOfMemory;
        generation.budget.limit -= size;
        self.jobs[self.count] = .{ .file = file, .blob = blob, .display = name, .explicit_file = explicit_file, .reserved = size };
        self.count += 1;
        self.bytes += blob.bytes.len;
    }
    fn clear(self: *CaptureQueue) void {
        const generation = self.collector.generation;
        for (self.jobs[0..self.count]) |job| {
            job.file.close(self.collector.io);
            if (job.blob) |blob| blob.release();
            generation.budget.limit += job.reserved;
            generation.budget.allocator().free(job.display);
        }
        self.count = 0;
        self.bytes = 0;
    }
    fn run(self: *CaptureQueue) void {
        while (true) {
            const i = self.cursor.fetchAdd(1, .monotonic);
            if (i >= self.count) return;
            const job = &self.jobs[i];
            job.revision = readBlob(self.collector.io, job.file, job.blob.?.stamp, job.blob.?) catch |err| {
                job.failure = err;
                continue;
            };
        }
    }
    fn flush(self: *CaptureQueue) !void {
        if (self.count == 0) return;
        defer self.clear();
        self.cursor.store(0, .monotonic);
        var handles: [8]@import("worker_pool.zig").Handle = undefined;
        var launched: usize = 0;
        if (self.count > 1) for (0..@min(self.workers, self.count) - 1) |_| {
            handles[launched] = self.pool.launch(run, self) catch break;
            launched += 1;
        };
        self.run();
        for (handles[0..launched]) |handle| handle.join();
        for (self.jobs[0..self.count]) |job| if (job.failure) |err| return err;
        if (launched > 0) {
            self.collector.generation.capture_workers = @max(self.collector.generation.capture_workers, launched + 1);
            self.collector.generation.parallel_sources_read += self.count;
        }
        for (self.jobs[0..self.count]) |*job| {
            self.collector.generation.budget.limit += job.reserved;
            job.reserved = 0;
            const blob = job.blob.?;
            job.blob = null;
            try self.collector.finishRead(blob, job.display, job.explicit_file, job.revision);
        }
    }
    fn deinit(self: *CaptureQueue) void {
        self.clear();
        self.pool.deinit();
    }
};

const Collector = struct {
    generation: *Generation,
    io: std.Io,
    options: *const Options,
    previous: ?*const Generation,
    watcher: ?*Watch = null,
    journal: ?*@import("discovery_journal.zig").Journal = null,
    trusted_membership: bool = false,
    membership_seen: ?*std.AutoHashMapUnmanaged(usize, void) = null,
    membership_visited: usize = 0,
    eligibility: ?*std.ArrayList([]const u8) = null,
    queue: ?*CaptureQueue = null,
    opens: ?*OpenQueue = null,
    fn markSeen(self: *Collector, path_name: []const u8) !void {
        if (self.membership_seen) |seen| if (self.previous.?.find(path_name)) |entry| {
            const slot = (@intFromPtr(entry) - @intFromPtr(self.previous.?.entries.items.ptr)) / @sizeOf(Entry);
            try seen.put(self.generation.arena.allocator(), slot, {});
        };
    }
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
            const display = if (std.mem.eql(u8, name, ".") and self.options.paths.items.len == 0) "" else if (name.len > 1) std.mem.trimEnd(u8, name, "/") else name;
            if (self.journal) |journal| journal.registerRoot(full, display);
            try discovery.walk(full, display, self);
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
        self.membership_visited += 1;
        if (self.opens) |opens| if (generation.entries.items.len +| (if (self.queue) |queue| queue.count else 0) +| opens.count >= self.options.workspace_max_files) try opens.flush();
        if (generation.entries.items.len +| (if (self.queue) |queue| queue.count else 0) >= self.options.workspace_max_files) return error.WorkspaceFileLimit;
        if (self.watcher) |watcher| if (if (self.previous) |old| old.find(display) else null) |old| {
            if (old.blob.watcher == watcher and old.blob.token != null and
                watcher.intact(old.blob.token.?) and
                (self.trusted_membership or watcher.identityMatchesPath(old.blob.token.?, path_name)))
            {
                if (self.membership_seen != null) {
                    try self.markSeen(display);
                    return;
                }
                const available = generation.budget.limit -| generation.budget.live;
                old.blob.refs += 1;
                errdefer old.blob.release();
                try self.append(old.blob, display, explicit_file, old.digest, true, available);
                generation.source_bytes_reused += old.bytes().len;
                return;
            }
        };
        generation.sources_opened += 1;
        if (self.opens) |opens| {
            // Flush before enqueueing so max-file accounting includes both
            // pending queues and the already-published generation exactly once.
            if (opens.count == opens.capacity) try opens.flush();
            try opens.push(path_name, display, explicit_file);
            return;
        }
        if (self.queue) |queue| if (queue.count == queue.capacity) try queue.flush();
        const file_handle = std.Io.Dir.cwd().openFile(self.io, path_name, .{}) catch |err| blk: {
            // Optional pending reads must not consume the last descriptors and
            // turn a source which fits serially into a capture failure.
            const queue = self.queue orelse return err;
            if (queue.count == 0) return err;
            try queue.flush();
            break :blk try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
        };
        const before = file_handle.stat(self.io) catch |err| {
            file_handle.close(self.io);
            return err;
        };
        return self.addOpened(file_handle, before, path_name, display, explicit_file);
    }
    // Consumes the descriptor on both success and failure.
    fn addOpened(self: *Collector, file_handle: std.Io.File, before: std.Io.File.Stat, path_name: []const u8, display: []const u8, explicit_file: bool) !void {
        var close_file = true;
        defer if (close_file) file_handle.close(self.io);
        const generation = self.generation;
        if (self.queue) |queue| if (queue.count == queue.capacity) try queue.flush();
        if (before.kind != .file) return error.NotRegularFile;
        if (before.size > self.options.max_file_bytes and self.options.skip_oversize) {
            // Size affects discovery eligibility without changing directory
            // membership. Observe excluded inputs as discovery dependencies.
            if (self.journal) |journal| {
                journal.dependency(path_name);
                const latest = try std.Io.Dir.cwd().statFile(self.io, path_name, .{});
                if (!sameStamp(before, latest)) return error.WorkspaceChangedDuringCapture;
            }
            if (self.eligibility) |list| try list.append(generation.arena.allocator(), try generation.arena.allocator().dupe(u8, path_name));
            return;
        }
        if (self.queue) |queue| if (queue.count != 0 and (before.size > CaptureQueue.byte_limit -| queue.bytes or before.size +| @sizeOf(Blob) > generation.budget.limit -| generation.budget.live)) try queue.flush();
        const available = generation.budget.limit -| generation.budget.live;
        if (available < @sizeOf(Blob)) return error.OutOfMemory;
        if (self.watcher) |watcher| if (if (self.previous) |old| old.find(display) else null) |old| {
            if (old.blob.token != null and old.blob.watcher == watcher and old.blob.stamp.valid and sameStamp(old.blob.stamp, before) and watcher.clean(old.blob.token.?, file_handle)) {
                if (old.blob.charge() > available) return error.OutOfMemory;
                old.blob.refs += 1;
                errdefer old.blob.release();
                try self.append(old.blob, display, explicit_file, old.digest, true, available);
                generation.source_bytes_reused += old.bytes().len;
                return;
            }
        };
        const token = if (self.watcher) |w| w.armCaptured(file_handle) else null;
        var token_owned = true;
        defer if (token_owned) if (token) |value| self.watcher.?.release(value);
        const length = std.math.cast(usize, before.size) orelse return error.StreamTooLong;
        if (length > self.options.max_file_bytes) return error.StreamTooLong;
        if (length > available - @sizeOf(Blob)) return error.OutOfMemory;
        var revision: [32]u8 = undefined;
        const blob = blk: {
            const value = try Blob.create(generation.backing, length);
            errdefer value.release();
            value.watcher = self.watcher;
            value.token = token;
            value.stamp = .from(before);
            token_owned = false;
            if (self.queue) |queue| if (length <= CaptureQueue.byte_limit and (!self.options.jobs_auto or length >= 64 * 1024)) {
                try queue.push(file_handle, value, display, explicit_file);
                close_file = false;
                return;
            };
            revision = try readBlob(self.io, file_handle, before, value);
            break :blk value;
        };
        try self.finishRead(blob, display, explicit_file, revision);
    }
    // Consumes the blob on both success and failure. Only the coordinator
    // mutates watch state, generation accounting, and prior-reference counts.
    fn finishRead(self: *Collector, captured: *Blob, display: []const u8, explicit_file: bool, revision: [32]u8) !void {
        const generation = self.generation;
        const available = generation.budget.limit -| generation.budget.live;
        generation.source_bytes_read += captured.bytes.len;
        if (captured.token) |token| self.watcher.?.certifyPending(token);
        var blob = captured;
        errdefer blob.release();
        const stamp = captured.stamp;
        if (if (self.previous) |old| old.find(display) else null) |old| {
            if (std.mem.eql(u8, &old.digest, &revision)) {
                if (old.blob.charge() > available) return error.OutOfMemory;
                const token = blob.token;
                blob.token = null;
                blob.release();
                blob = old.blob;
                blob.refs += 1;
                if (blob.watcher) |w| if (blob.token) |prior| w.release(prior);
                blob.watcher = self.watcher;
                blob.token = token;
            }
        }
        blob.stamp = stamp;
        try self.append(blob, display, explicit_file, revision, false, available);
    }
    fn sameStamp(a: anytype, b: std.Io.File.Stat) bool {
        return a.inode == b.inode and a.kind == b.kind and a.size == b.size and a.mtime.nanoseconds == b.mtime.nanoseconds and a.ctime.nanoseconds == b.ctime.nanoseconds;
    }
    fn append(self: *Collector, blob: *Blob, display: []const u8, explicit_file: bool, revision: [32]u8, witness_reused: bool, available: usize) !void {
        const generation = self.generation;
        try self.markSeen(display);
        const previous = if (self.previous) |old| old.find(display) else null;
        const preparation = if (previous != null and std.mem.eql(u8, &previous.?.digest, &revision)) previous.?.preparation else null;
        const charge = blob.charge() + (if (preparation) |owner| owner.bytes() else @as(usize, 0));
        if (charge > available) return error.OutOfMemory;
        generation.budget.limit -= charge;
        generation.blob_bytes += charge;
        errdefer {
            generation.budget.limit += charge;
            generation.blob_bytes -= charge;
        }
        const owned_path = try generation.budget.allocator().dupe(u8, display);
        errdefer generation.budget.allocator().free(owned_path);
        try generation.entries.append(generation.budget.allocator(), .{ .blob = blob, .path = owned_path, .digest = revision, .explicit_file = explicit_file, .witness_reused = witness_reused, .preparation = preparation });
        generation.source_bytes += blob.bytes.len;
        generation.path_bytes += display.len;
        if (preparation) |owner| {
            owner.refs += 1;
            generation.preparations_reused += 1;
            generation.preparation_count += 1;
        }
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
    try std.testing.expectEqualStrings("retry", next.entries.items[0].prepared().?.index.declarations[0].name);
    try std.testing.expectEqualStrings("STOP", next.entries.items[0].prepared().?.index.calls[0].name);
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
    try std.testing.expectEqualStrings("def old():\n    STOP()\n", old.entries.items[0].bytes());
    try std.testing.expectEqualStrings("def changed():\n    STOP()\n", next.entries.items[0].bytes());
    try std.testing.expectEqual(@as(usize, 0), next.preparations_built);
    try std.testing.expect(next.prepareEntry(io, &next.entries.items[0]));
    try std.testing.expect(next.retainedBytes() <= o.workspace_memory_limit);
    try std.testing.expect(old.entries.items[0].preparation == null);
    try std.testing.expectEqualStrings("changed", next.entries.items[0].prepared().?.index.declarations[0].name);
}

test "fallback revalidates an earlier clean namespace certificate before publishing" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a.txt", .data = "before" });
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
    if (!journal.clean()) return error.SkipZigTest;
    // Deterministically reproduce events observed after the initial clean
    // checkpoint, exactly where a rejected dirty fast path enters fallback.
    try tmp.dir.writeFile(io, .{ .sub_path = "a.txt", .data = "after" });
    try tmp.dir.writeFile(io, .{ .sub_path = "new.txt", .data = "newneedle" });
    const next = try Generation.captureFallback(a, io, &o, old, &watch, &journal, true);
    defer next.deinit();
    try std.testing.expect(!next.discovery_reused);
    try std.testing.expectEqual(@as(usize, 2), next.entries.items.len);
    try std.testing.expectEqualStrings("newneedle", next.entries.items[1].bytes());
}

test "atomic replacement at inode admission limit cannot retain a false clean certificate" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a.txt", .data = "before" });
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    var o: Options = .{ .no_ignore = true };
    defer o.deinit(a);
    try o.paths.append(a, path);
    var watch = Watch.init();
    defer watch.deinit();
    watch.limit = 1;
    var journal = @import("discovery_journal.zig").Journal.init(a, io);
    defer journal.deinit();
    var current = try Generation.captureJournal(a, io, &o, null, &watch, &journal);
    defer current.deinit();
    if (!journal.sources_covered) return error.SkipZigTest;
    try tmp.dir.writeFile(io, .{ .sub_path = ".replacement", .data = "replacement" });
    try tmp.dir.rename(".replacement", tmp.dir, "a.txt", io);
    const next = try Generation.captureJournal(a, io, &o, current, &watch, &journal);
    current.deinit();
    current = next;
    try std.testing.expectEqualStrings("replacement", current.entries.items[0].bytes());
    try tmp.dir.writeFile(io, .{ .sub_path = "a.txt", .data = "immediate next write" });
    const changed = try Generation.captureJournal(a, io, &o, current, &watch, &journal);
    current.deinit();
    current = changed;
    try std.testing.expectEqualStrings("immediate next write", current.entries.items[0].bytes());
}

test "unwitnessed hard links cannot use directory-only content certification" {
    if (@import("builtin").os.tag != .linux and @import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDir(io, "src", .default_dir);
    try tmp.dir.writeFile(io, .{ .sub_path = "outside", .data = "before" });
    try tmp.dir.hardLink("outside", tmp.dir, "src/a.txt", io, .{});
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/src", .{tmp.sub_path});
    defer a.free(path);
    var o: Options = .{ .no_ignore = true };
    defer o.deinit(a);
    try o.paths.append(a, path);
    var watch = Watch.init();
    defer watch.deinit();
    watch.limit = 0;
    var journal = @import("discovery_journal.zig").Journal.init(a, io);
    defer journal.deinit();
    const old = try Generation.captureJournal(a, io, &o, null, &watch, &journal);
    defer old.deinit();
    try std.testing.expect(old.watch_fallback);
    try std.testing.expect(!journal.sources_covered);
    try tmp.dir.writeFile(io, .{ .sub_path = "outside", .data = "changed through another name" });
    const next = try Generation.captureJournal(a, io, &o, old, &watch, &journal);
    defer next.deinit();
    try std.testing.expectEqualStrings("changed through another name", next.entries.items[0].bytes());
    try std.testing.expect(next.source_bytes_read != 0);
}

test "size-excluded inputs invalidate discovery when they become eligible" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a.txt", .data = "too large" });
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    var o: Options = .{ .no_ignore = true, .max_file_bytes = 4, .skip_oversize = true };
    defer o.deinit(a);
    try o.paths.append(a, path);
    var watch = Watch.init();
    defer watch.deinit();
    var journal = @import("discovery_journal.zig").Journal.init(a, io);
    defer journal.deinit();
    const old = try Generation.captureJournal(a, io, &o, null, &watch, &journal);
    defer old.deinit();
    try std.testing.expectEqual(@as(usize, 0), old.entries.items.len);
    try tmp.dir.writeFile(io, .{ .sub_path = "a.txt", .data = "hit" });
    const next = try Generation.captureJournal(a, io, &o, old, &watch, &journal);
    defer next.deinit();
    try std.testing.expectEqual(@as(usize, 1), next.entries.items.len);
    try std.testing.expectEqualStrings("hit", next.entries.items[0].bytes());
}

test "membership updates retain canonical identities and visit only affected directories" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buffer: [128]u8 = undefined;
    for (0..32) |i| {
        const dir = try std.fmt.bufPrint(&buffer, "dir-{d}", .{i});
        try tmp.dir.createDir(io, dir, .default_dir);
        const name = try std.fmt.bufPrint(&buffer, "dir-{d}/original.txt", .{i});
        try tmp.dir.writeFile(io, .{ .sub_path = name, .data = "original\n" });
    }
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    var o: Options = .{ .no_ignore = true };
    defer o.deinit(a);
    try o.paths.append(a, path);
    var watch = Watch.init();
    defer watch.deinit();
    var journal = @import("discovery_journal.zig").Journal.init(a, io);
    defer journal.deinit();
    var current = try Generation.captureJournal(a, io, &o, null, &watch, &journal);
    defer current.deinit();
    if (!journal.sources_covered or !journal.coverage) return error.SkipZigTest;
    const initial_bytes = current.retainedBytes();
    for (0..24) |iteration| {
        const added = try std.fmt.bufPrint(&buffer, "dir-{d}/new.txt", .{iteration % 32});
        try tmp.dir.writeFile(io, .{ .sub_path = added, .data = "new\n" });
        if (iteration > 0) {
            const removed = try std.fmt.bufPrint(&buffer, "dir-{d}/new.txt", .{(iteration - 1) % 32});
            try tmp.dir.deleteFile(io, removed);
        }
        const next = try Generation.captureJournal(a, io, &o, current, &watch, &journal);
        current.deinit();
        current = next;
        try std.testing.expect(current.discovery_reused);
        try std.testing.expect(current.refresh_entries_visited <= 5);
        // Coalesced ancestor notifications can require rechecking every
        // canonical pathname, especially while other suites mutate tmp's
        // siblings. Only affected directories may be enumerated or revisited.
        try std.testing.expect(current.namespace_directories_scanned <= 5);
        const rebuilt = try Generation.capture(a, io, &o, null);
        defer rebuilt.deinit();
        try std.testing.expectEqual(rebuilt.path_bytes, current.path_bytes);
        try std.testing.expectEqualSlices(u8, &rebuilt.revision, &current.revision);
        var ordered = current.iterator();
        var expected = rebuilt.iterator();
        while (ordered.next()) |entry| {
            const reference = expected.next().?;
            try std.testing.expectEqualStrings(reference.path, entry.path);
            try std.testing.expectEqualStrings(reference.bytes(), entry.bytes());
        }
        try std.testing.expect(expected.next() == null);
    }
    // Repeated updates retire paths/tree nodes and reuse bounded dense capacity.
    try std.testing.expect(current.retainedBytes() < initial_bytes + 96 * 1024);
}

test "parallel cold capture equals serial capture and bounds failed admission" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    const payload = try a.alloc(u8, 128 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    for (0..40) |i| {
        var name: [32]u8 = undefined;
        try tmp.dir.writeFile(io, .{ .sub_path = try std.fmt.bufPrint(&name, "f{d}.txt", .{i}), .data = payload });
    }
    var options: Options = .{ .no_ignore = true, .jobs = 1, .workspace_parallel_open = true, .workspace_memory_limit = 16 * 1024 * 1024 };
    defer options.deinit(a);
    try options.paths.append(a, path);
    const serial = try Generation.capture(a, io, &options, null);
    defer serial.deinit();
    options.jobs = 4;
    const parallel = try Generation.capture(a, io, &options, null);
    defer parallel.deinit();
    try std.testing.expectEqualSlices(u8, &serial.revision, &parallel.revision);
    try std.testing.expectEqual(serial.source_bytes, parallel.source_bytes);
    try std.testing.expectEqual(serial.path_bytes, parallel.path_bytes);
    try std.testing.expectEqual(@as(usize, 0), serial.capture_workers);
    try std.testing.expectEqual(@as(usize, 4), parallel.capture_workers);
    try std.testing.expectEqual(@as(usize, 40), parallel.parallel_sources_read);
    try std.testing.expectEqual(serial.retainedBytes(), parallel.retainedBytes());
    options.workspace_max_files = 39;
    try std.testing.expectError(error.WorkspaceFileLimit, Generation.capture(a, io, &options, null));
    options.workspace_max_files = 40;
    options.workspace_memory_limit = 128 * 1024;
    try std.testing.expectError(error.OutOfMemory, Generation.capture(a, io, &options, null));
}

test "parallel reader failure retires pending reservations without publication" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "old" });
    const file = try tmp.dir.openFile(io, "a", .{});
    const before = try file.stat(io);
    const blob = try Blob.create(a, 3);
    blob.stamp = .from(before);
    var generation: Generation = .{ .backing = a, .budget = .{ .backing = a, .limit = 1024 * 1024 }, .arena = undefined };
    var collector: Collector = .{ .generation = &generation, .io = io, .options = &.{}, .previous = null };
    var queue: CaptureQueue = .{ .collector = &collector, .pool = .{ .io = io }, .workers = 2 };
    defer queue.deinit();
    const limit = generation.budget.limit;
    try queue.push(file, blob, "a", false);
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "longer" });
    try std.testing.expectError(error.WorkspaceChangedDuringCapture, queue.flush());
    try std.testing.expectEqual(@as(usize, 0), generation.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), generation.budget.live);
    try std.testing.expectEqual(limit, generation.budget.limit);
}

fn coldFailureCase(a: A, path: []const u8) !void {
    var options: Options = .{ .no_ignore = true, .jobs = 4, .workspace_parallel_open = true };
    defer options.deinit(a);
    try options.paths.append(a, path);
    const generation = try Generation.capture(a, std.testing.io, &options, null);
    generation.deinit();
}
test "parallel cold capture allocation failures release descriptors and immutable blobs" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "empty", .data = "" });
    try tmp.dir.writeFile(io, .{ .sub_path = "small", .data = "STOP" });
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    try std.testing.checkAllAllocationFailures(a, coldFailureCase, .{path});
}

test "source blob stamp validity shares existing header padding" {
    // Optional Stamp needs a separate 16-byte tag on 64-bit targets. Its
    // validity bit fits beside the other header flags without that overhead.
    const Tagged = struct { backing: A, refs: usize, bytes: []u8, stamp: ?Stamp, watcher: ?*Watch, token: ?Token };
    if (@sizeOf(usize) == 8) try std.testing.expect(@sizeOf(Blob) < @sizeOf(Tagged));
    const blob = try Blob.create(std.testing.allocator, 0);
    defer blob.release();
    try std.testing.expect(!blob.stamp.valid);
}

test "parallel open failures release descriptors and owned discovery paths" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "STOP" });
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/a", .{tmp.sub_path});
    defer a.free(path);
    const missing = try std.fmt.allocPrint(a, "{s}-missing", .{path});
    defer a.free(missing);
    var generation: Generation = .{ .backing = a, .budget = .{ .backing = a, .limit = 1024 * 1024 }, .arena = undefined };
    var collector: Collector = .{ .generation = &generation, .io = io, .options = &.{}, .previous = null };
    var pool: @import("worker_pool.zig").Pool = .{ .io = io };
    defer pool.deinit();
    var opens: OpenQueue = .{ .collector = &collector, .pool = &pool, .workers = 2, .capacity = 2 };
    defer opens.deinit();
    try opens.push(missing, "missing", false);
    try opens.push(path, path, false);
    try std.testing.expectError(error.FileNotFound, opens.flush());
    try std.testing.expectEqual(@as(usize, 0), generation.budget.live);
    try std.testing.expectEqual(@as(usize, 0), opens.count);
}

test "pending oversized opens do not consume the eligible file limit" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "large", .data = "too large" });
    try tmp.dir.writeFile(io, .{ .sub_path = "small", .data = "hit" });
    var options: Options = .{ .no_ignore = true, .jobs = 4, .workspace_parallel_open = true, .max_file_bytes = 3, .skip_oversize = true, .workspace_max_files = 1 };
    defer options.deinit(a);
    for ([_][]const u8{ "large", "small" }) |name| try options.paths.append(a, try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name }));
    defer for (options.paths.items) |path| a.free(path);
    const generation = try Generation.capture(a, io, &options, null);
    defer generation.deinit();
    try std.testing.expectEqual(@as(usize, 1), generation.entries.items.len);
    try std.testing.expectEqualStrings("hit", generation.entries.items[0].bytes());
}
