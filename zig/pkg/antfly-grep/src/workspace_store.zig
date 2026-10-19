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
    bytes: []const u8,
    revision: [64]u8,
    explicit_file: bool = false,
    witness_reused: bool = false,
    watch_next: usize = std.math.maxInt(usize),
    watch_prev: usize = std.math.maxInt(usize),
    watch_slot: usize = std.math.maxInt(usize),
    touched: u64 = 0,
    directory_next: usize = std.math.maxInt(usize),
    directory_prev: usize = std.math.maxInt(usize),
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
        self.discovery_reused = trusted;
        if (watcher) |w| w.poll();
        if (trusted) {
            for (previous.?.entries.items) |entry| try collector.addFile(entry.path, entry.path, entry.explicit_file);
        } else if (o.paths.items.len == 0) try collector.path(".") else for (o.paths.items) |path| try collector.path(path);
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
            self.watch_heads = try allocator.alloc(usize, watcher.?.limit);
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
                if (order != .eq or !std.mem.eql(u8, &left.?.revision, &right.?.revision) or left.?.explicit_file != right.?.explicit_file)
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
        h.update(&entry.revision);
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
        for (w.events[0..event_count]) |slot| {
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
            source_bytes = source_bytes - entry.bytes.len + next.bytes.len;
            retained = retained - entry.charge() + next.charge();
            if (!std.mem.eql(u8, &entry.revision, &next.revision)) try appendChange(old.budget.allocator(), &changes, entry.path);
            const updated = try inventory.put(old.budget.allocator(), tree, entry.path, i, leafHash(next));
            inventory.release(old.budget.allocator(), tree);
            tree = updated;
        }
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
            if (entry.explicit_file or (entry.blob.stamp != null and entry.blob.stamp.?.nlink > 1)) requires_inode_witness = true;
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
            old.preparation_count = old.preparation_count - @intFromBool(prior.prepared != null) + @intFromBool(next.prepared != null);
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
        for (j.membership_dirs.items) |directory| {
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
                var events_at = j.native.paths.keyIterator();
                while (events_at.next()) |name| if (std.mem.eql(u8, std.fs.path.dirname(name.*) orelse "/", directory.path)) {
                    const owned = scratch.dupe(u8, name.*) catch {
                        j.native.unlock();
                        return null;
                    };
                    paths.append(scratch, owned) catch {
                        j.native.unlock();
                        return null;
                    };
                };
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
        for (j.membership_dirs.items) |directory| {
            if (directory.native_names) {
                for (considered.items) |path| if (std.mem.eql(u8, std.fs.path.dirname(path) orelse "", directory.display)) {
                    if (old.find(path)) |entry| {
                        const slot = (@intFromPtr(entry) - @intFromPtr(old.entries.items.ptr)) / @sizeOf(Entry);
                        if (!seen.contains(slot)) try removed.append(scratch, entry.path);
                    }
                };
                continue;
            }
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
        for (w.events[0..event_count]) |watch_slot| {
            var slot = old.watch_heads[watch_slot];
            while (slot != std.math.maxInt(usize)) : (slot = old.entries.items[slot].watch_next) {
                const entry = old.entries.items[slot];
                var scanned = false;
                for (j.membership_dirs.items) |directory| if (std.mem.eql(u8, std.fs.path.dirname(entry.path) orelse "", directory.display)) {
                    scanned = true;
                    break;
                };
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
            var scanned = false;
            for (j.membership_dirs.items) |directory| if (std.mem.eql(u8, std.fs.path.dirname(entry.path) orelse "", directory.display)) {
                scanned = true;
                break;
            };
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
            bytes -= entry.bytes.len;
            preparations -= @intFromBool(entry.prepared != null);
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
                bytes -= prior.bytes.len;
                preparations -= @intFromBool(prior.prepared != null);
                if (!std.mem.eql(u8, &prior.revision, &entry.revision) or prior.explicit_file != entry.explicit_file) try appendChange(old.budget.allocator(), &changes, entry.path);
            } else {
                if (count >= o.workspace_max_files) return null;
                count += 1;
                try appendChange(old.budget.allocator(), &changes, entry.path);
            }
            retained += entry.charge();
            bytes += entry.bytes.len;
            preparations += @intFromBool(entry.prepared != null);
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
            if (@import("builtin").os.tag != .linux or entry.explicit_file or (entry.blob.stamp != null and entry.blob.stamp.?.nlink > 1) or !j.native_covered or !j.allow_native_sources) j.sources_covered = false;
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
    membership_seen: ?*std.AutoHashMapUnmanaged(usize, void) = null,
    membership_visited: usize = 0,
    eligibility: ?*std.ArrayList([]const u8) = null,
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
        if (generation.entries.items.len >= self.options.workspace_max_files) return error.WorkspaceFileLimit;
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
        if (if (self.previous) |old| old.find(display) else null) |old| {
            if (std.mem.eql(u8, &old.revision, &revision)) {
                const retained = old.blob.budget.live + @sizeOf(Blob);
                if (retained > available) return error.OutOfMemory;
                blob.release();
                blob = old.blob;
                blob.refs += 1;
            }
        }
        if (blob.watcher) |w| if (blob.token) |old| w.release(old);
        blob.watcher = self.watcher;
        blob.token = token;
        blob.stamp = before;
        token = null;
        try self.append(blob, display, explicit_file, revision, false, available);
    }
    fn sameStamp(a: std.Io.File.Stat, b: std.Io.File.Stat) bool {
        return a.inode == b.inode and a.kind == b.kind and a.size == b.size and a.mtime.nanoseconds == b.mtime.nanoseconds and a.ctime.nanoseconds == b.ctime.nanoseconds;
    }
    fn append(self: *Collector, blob: *Blob, display: []const u8, explicit_file: bool, revision: [64]u8, witness_reused: bool, available: usize) !void {
        const generation = self.generation;
        try self.markSeen(display);
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
        const owned_path = try generation.budget.allocator().dupe(u8, display);
        errdefer generation.budget.allocator().free(owned_path);
        try generation.entries.append(generation.budget.allocator(), .{ .blob = blob, .path = owned_path, .bytes = blob.bytes, .revision = revision, .explicit_file = explicit_file, .witness_reused = witness_reused, .prepared = if (preparation) |owner| owner.value else null, .preparation = preparation });
        generation.source_bytes += blob.bytes.len;
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
    try std.testing.expectEqualStrings("newneedle", next.entries.items[1].bytes);
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
    try std.testing.expectEqualStrings("replacement", current.entries.items[0].bytes);
    try tmp.dir.writeFile(io, .{ .sub_path = "a.txt", .data = "immediate next write" });
    const changed = try Generation.captureJournal(a, io, &o, current, &watch, &journal);
    current.deinit();
    current = changed;
    try std.testing.expectEqualStrings("immediate next write", current.entries.items[0].bytes);
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
    try std.testing.expectEqualStrings("changed through another name", next.entries.items[0].bytes);
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
    try std.testing.expectEqualStrings("hit", next.entries.items[0].bytes);
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
        try std.testing.expect(current.namespace_paths_checked < 24);
        const rebuilt = try Generation.capture(a, io, &o, null);
        defer rebuilt.deinit();
        try std.testing.expectEqualSlices(u8, &rebuilt.revision, &current.revision);
        var ordered = current.iterator();
        var expected = rebuilt.iterator();
        while (ordered.next()) |entry| {
            const reference = expected.next().?;
            try std.testing.expectEqualStrings(reference.path, entry.path);
            try std.testing.expectEqualStrings(reference.bytes, entry.bytes);
        }
        try std.testing.expect(expected.next() == null);
    }
    // Repeated updates retire paths/tree nodes and reuse bounded dense capacity.
    try std.testing.expect(current.retainedBytes() < initial_bytes + 96 * 1024);
}
