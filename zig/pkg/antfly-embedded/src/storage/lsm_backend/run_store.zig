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

//! Writer ownership, ordered independently of contiguous positions. Candidate
//! roots share untouched payloads; adoption occurs only after every fallible
//! publication preparation. Mutable cache hints belong to these writer-owned
//! runs, not to the immutable reader directory.
const std = @import("std");
const Run = @import("repository.zig").Run;
const Directory = @import("run_directory.zig").Directory;
const Account = @import("memory_account.zig").Account;
const DestroyRun = *const fn (*Run, std.mem.Allocator) void;
const admission = @import("../../common/read_admission.zig");
const pins = @import("run_pin_registry.zig");
const snapshots = @import("run_snapshot_clone.zig");
const Backend = @import("../lsm_backend.zig").Backend;
const RunOwner = @import("repository.zig").RunOwner;

/// Slice-backed planner fixtures remain an independent oracle. Production
/// uses the ordered owner; these adapters never materialize a hidden vector.
pub fn count(backend: anytype) usize {
    return if (@TypeOf(backend.runs) == Store) backend.runs.count() else backend.runs.items.len;
}
pub fn at(backend: anytype, rank: usize) *Run {
    return if (@TypeOf(backend.runs) == Store) backend.runs.at(rank) else &backend.runs.items[rank];
}
pub fn project(backend: anytype, allocator: std.mem.Allocator) ![]Run {
    return if (@TypeOf(backend.runs) == Store) backend.runs.project(allocator) else allocator.dupe(Run, backend.runs.items);
}
/// Stable plan handles address writer records across unrelated rank changes.
/// Positions remain only for slice-backed planner oracles.
pub fn planAt(backend: anytype, plan: anytype, i: usize) *Run {
    if (comptime @TypeOf(backend.runs) == Store) {
        if (plan.input_handles) |handles| {
            const offset = if (i < plan.source_len) plan.source_start + i else plan.target_start + i - plan.source_len;
            return backend.runs.find(handles[offset].run).?;
        }
    }
    return at(backend, if (i < plan.source_len) plan.sourceIndex(i) else plan.targetIndex(i - plan.source_len));
}

pub fn planGet(runs: anytype, plan: anytype, i: usize) Run {
    if (comptime @TypeOf(runs) == *Store or @TypeOf(runs) == *const Store) {
        if (plan.input_handles) |handles| {
            const offset = if (i < plan.source_len) plan.source_start + i else plan.target_start + i - plan.source_len;
            return runs.find(handles[offset].run).?.*;
        }
    }
    return get(runs, if (i < plan.source_len) plan.sourceIndex(i) else plan.targetIndex(i - plan.source_len));
}
/// The independent flat planner is exercised only by differential tests.
/// Production cannot silently fall back to materializing its entire run set.
pub fn oracleItems(backend: anytype) ![]const Run {
    if (comptime @TypeOf(backend.runs) != Store) return backend.runs.items;
    if (comptime @import("builtin").is_test) return backend.runs.testItems(backend.allocator);
    return error.TestOnlyPlanner;
}
pub fn len(runs: anytype) usize {
    if (comptime @TypeOf(runs) == *Store or @TypeOf(runs) == *const Store) return runs.count();
    if (comptime @TypeOf(runs) == *std.ArrayListUnmanaged(Run) or @TypeOf(runs) == *const std.ArrayListUnmanaged(Run)) return runs.items.len;
    return runs.len;
}
pub fn get(runs: anytype, rank: usize) Run {
    if (comptime @TypeOf(runs) == *Store or @TypeOf(runs) == *const Store) return runs.at(rank).*;
    if (comptime @TypeOf(runs) == *std.ArrayListUnmanaged(Run) or @TypeOf(runs) == *const std.ArrayListUnmanaged(Run)) return runs.items[rank];
    return runs[rank];
}

const RetirementWorkspace = struct {
    phase: enum { start, version_forget, snapshot_forget, physical, finish } = .start,
    forgetting: ?pins.Operation = null,
    stale: bool = false,
    physical: ?snapshots.Retirement = null,

    /// Forget never creates registry metadata. A stale plan owns its partial
    /// retirement before retry; no port or Backend receiver escapes this call.
    fn forget(self: *@This(), operation: pins.Operation, original: ?admission.Port) !bool {
        if (self.forgetting == null) self.forgetting = operation;
        const saved = &self.forgetting.?;
        if (!self.stale and !saved.committed) {
            const result: anyerror!void = work: {
                const bytes = saved.planWithAdmission(original) catch |cause| break :work cause;
                if (bytes != 0) break :work error.ResourceAccountingMismatch;
                saved.resumeWithAdmission(original) catch |cause| break :work cause;
                saved.publishWithAdmission(original) catch |cause| break :work cause;
                break :work {};
            };
            result catch |cause| {
                if (cause != error.CompactionPlanningStale) return cause;
                self.stale = true;
                return false;
            };
        }
        try saved.retireWithAdmission(original);
        self.forgetting = null;
        if (self.stale) {
            self.stale = false;
            return false;
        }
        return true;
    }
};

const Payload = struct {
    owner: @import("repository.zig").RunOwner,
    parent: ?*@import("repository.zig").RunOwner = null,
    run: Run,
    owned: bool = false,
    destroy_run: ?DestroyRun,
    account: *Account,
    bytes: u64,
    version_pin: ?*pins.Registry.Pin = null,
    retirement: RetirementWorkspace = .{},

    fn destroy(header: *@import("repository.zig").RunOwner, allocator: std.mem.Allocator) void {
        const payload: *Payload = @alignCast(@fieldParentPtr("owner", header));
        if (payload.version_pin) |pin| {
            pin.release();
            payload.version_pin = null;
            payload.run.version_ref_pinned = false;
        }
        if (payload.parent) |parent| {
            if (payload.run.owns_bloom_filter) if (payload.run.bloom_filter) |*filter| filter.deinit(allocator);
            if (payload.run.table_index) |*index| index.deinit(allocator);
            if (payload.run.path != null) if (payload.run.state) |*state| state.deinit(allocator);
            parent.release(allocator);
        } else if (payload.owned) {
            payload.run.owner = null;
            if (payload.destroy_run) |destroy_run| destroy_run(&payload.run, allocator) else payload.run.deinit(allocator);
        }
        payload.account.discharge(payload.bytes);
        allocator.destroy(payload);
    }
};

test "writer run store stages ownership atomically through allocation failures" {
    const Fixture = struct {
        fn make(allocator: std.mem.Allocator, id: u64) !Run {
            const key = try allocator.alloc(u8, 8);
            errdefer allocator.free(key);
            std.mem.writeInt(u64, key[0..8], id, .big);
            return .{ .id = id, .level = 1, .size_bytes = id, .path = null, .smallest_namespace_name = null, .smallest_key = key, .largest_namespace_name = null, .largest_key = try allocator.dupe(u8, key), .entry_count = 1, .bloom_filter = null, .state = null };
        }
        fn check(allocator: std.mem.Allocator) !void {
            var live: Store = .{};
            defer live.deinit(allocator);
            for (0..8) |i| {
                var run = try make(allocator, i + 1);
                errdefer run.deinit(allocator);
                try live.append(allocator, run);
            }
            var output = try make(allocator, 20);
            var adopted = false;
            defer if (!adopted) output.deinit(allocator);
            var candidate = live.fork();
            defer candidate.deinit(allocator);
            const removed = live.at(3);
            try candidate.remove(allocator, removed);
            try candidate.stage(allocator, output);
            try std.testing.expectEqual(@as(u64, 4), live.at(3).id);
            try std.testing.expectEqual(@as(u64, 5), candidate.at(3).id);
            try std.testing.expect(live.at(0) == candidate.at(0));
            candidate.adopt(&output);
            adopted = true;
            std.mem.swap(Store, &live, &candidate);
            try std.testing.expectEqual(@as(usize, 8), live.count());
            try std.testing.expectEqual(@as(u64, 52), live.totalBytes());
            // The original root is still pinned and owns its removed input.
            try std.testing.expectEqual(@as(u64, 4), candidate.at(3).id);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}
const Entry = struct {
    run: *Run,
    payload: ?*Payload = null,
    pub const Summary = struct { run_bytes: u64 = 0, manifest_bytes: u64 = 0, l0_files: usize = 0, l0_bytes: u64 = 0 };
    pub fn summarize(entry: Entry, left: Summary, right: Summary) Summary {
        const run = entry.run;
        const wire: u64 = if (run.path) |path| 112 + path.len + run.smallest_key.len + run.largest_key.len +
            (if (run.smallest_namespace_name) |name| name.len else 0) + (if (run.largest_namespace_name) |name| name.len else 0) else 0;
        return .{ .run_bytes = left.run_bytes +| right.run_bytes +| run.size_bytes, .manifest_bytes = left.manifest_bytes +| right.manifest_bytes +| wire, .l0_files = left.l0_files + right.l0_files + @intFromBool(run.level == 0), .l0_bytes = left.l0_bytes +| right.l0_bytes +| if (run.level == 0) run.size_bytes else 0 };
    }
    pub fn retainShared(self: Entry) Entry {
        _ = self.run.owner.?.retain();
        return self;
    }
    pub fn deinit(self: Entry, allocator: std.mem.Allocator) void {
        if (self.run.owner) |owner| owner.release(allocator);
    }
    /// Own one detached entry reference. Parent references transfer into this
    /// cursor instead of recursively invoking physical RunOwner destruction.
    pub const Retirement = struct {
        owner: ?*RunOwner,
        claimed: bool = false,
        pub fn init(entry: Entry) @This() {
            const owner = entry.run.owner orelse return .{ .owner = null };
            // Preserve the fixed generic-drop contract: registered anchors and
            // shared writers finish within the structural node credit. Only
            // the exclusive final physical owner needs later work credits.
            if (owner.refs.fetchSub(1, .acq_rel) != 1) return .{ .owner = null };
            return .{ .owner = owner, .claimed = true };
        }
        pub fn step(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
            if (self.owner == null) return true;
            while (credits.* != 0) if (self.stepWithAdmission(allocator, credits, null) catch unreachable) return true;
            return false;
        }
        fn isolated(payload: *Payload) Run {
            var run = payload.run;
            run.owner = null;
            run.version_ref_pinned = false;
            if (payload.parent != null) {
                // Revision metadata/state remain owned by the retained parent;
                // only this revision's private file caches belong to the child.
                run.output_ticket = null;
                run.shared_memory = null;
                run.shared_read_version = false;
                run.owns_metadata = false;
                run.owns_path = false;
                if (run.path == null) run.state = null;
            }
            return run;
        }
        pub fn stepWithAdmission(self: *@This(), allocator: std.mem.Allocator, credits: *usize, original: ?admission.Port) !bool {
            const owner = self.owner orelse return true;
            if (credits.* == 0) return false;
            if (original) |port| try port.checkpoint();
            credits.* -= 1;
            if (!self.claimed) {
                self.claimed = true;
                if (owner.refs.fetchSub(1, .acq_rel) != 1) {
                    self.owner = null;
                    return true;
                }
            }
            if (owner.destroy != Payload.destroy) {
                if (original != null) return error.ExecutionWaitUnsupported;
                owner.destroy(owner, allocator);
                self.owner = null;
                return true;
            }
            const payload: *Payload = @alignCast(@fieldParentPtr("owner", owner));
            comptime std.debug.assert(@sizeOf(Payload) <= 16 * 1024);
            const work = &payload.retirement;
            switch (work.phase) {
                .start => {
                    const producer = payload.parent == null and payload.owned;
                    const known = payload.destroy_run == null or payload.destroy_run == Backend.destroyOwnedRun;
                    const supported = !producer or (known and snapshots.Retirement.supports(isolated(payload)) and
                        (!payload.run.version_ref_pinned or (payload.destroy_run == Backend.destroyOwnedRun and payload.version_pin != null)));
                    if (!supported) {
                        if (original != null) return error.ExecutionWaitUnsupported;
                        Payload.destroy(owner, allocator);
                        self.owner = null;
                        return true;
                    }
                    if (payload.parent != null) work.phase = .physical else if (!payload.owned) work.phase = .finish else {
                        if (payload.version_pin) |pin| {
                            pin.release();
                            payload.version_pin = null;
                            payload.run.version_ref_pinned = false;
                        }
                        work.phase = if (payload.destroy_run == Backend.destroyOwnedRun) .version_forget else .physical;
                    }
                    return false;
                },
                .version_forget => {
                    if (payload.run.path) |path| if (!try work.forget(Backend.prepareVersionPinForget(path), original)) return false;
                    work.phase = .snapshot_forget;
                    return false;
                },
                .snapshot_forget => {
                    if (payload.run.path) |path| if (!try work.forget(Backend.prepareRunSnapshotRefOperation(path, .forget), original)) return false;
                    work.phase = .physical;
                    return false;
                },
                .physical => {
                    if (work.physical == null) {
                        const physical = try snapshots.Retirement.init(isolated(payload));
                        work.physical = physical;
                    }
                    if (!try work.physical.?.step(allocator, original)) return false;
                    work.phase = .finish;
                    return false;
                },
                .finish => {
                    const parent = payload.parent;
                    const account = payload.account;
                    const bytes = payload.bytes;
                    allocator.destroy(payload);
                    account.discharge(bytes);
                    self.owner = parent;
                    self.claimed = false;
                    return parent == null;
                },
            }
        }
    };
    pub fn retainedBytes(_: Entry) usize {
        return 0;
    }
};
fn compare(a: Entry, b: Entry) std.math.Order {
    if (Directory.runLess(a.run, b.run)) return .lt;
    if (Directory.runLess(b.run, a.run)) return .gt;
    return .eq;
}

pub const Store = struct {
    pub const Tree = @import("ordered_index.zig").Index(Entry, compare);
    /// Bind the actual canonical legacy writer token, not a borrowed revision.
    pub fn installVersionPin(run: *Run, pin: *pins.Registry.Pin, original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
        if (!Backend.isVersionPin(pin)) return error.InvalidArgument;
        const path = run.path orelse return error.InvalidArgument;
        const registered_path = pin.path();
        if (path.len != registered_path.len) return error.InvalidArgument;
        var offset: usize = 0;
        while (offset < path.len) {
            if (original) |port| try port.checkpoint();
            const end = @min(path.len, offset + 16 * 1024);
            if (!std.mem.eql(u8, path[offset..end], registered_path[offset..end])) return error.InvalidArgument;
            offset = end;
        }
        const owner = run.owner orelse return error.ExecutionWaitUnsupported;
        if (owner.destroy != Payload.destroy or owner.raw != run) return error.ExecutionWaitUnsupported;
        const payload: *Payload = @alignCast(@fieldParentPtr("owner", owner));
        if (!payload.owned or payload.parent != null or payload.destroy_run != Backend.destroyOwnedRun or payload.version_pin != null or run.version_ref_pinned) return error.InvalidArgument;
        payload.version_pin = pin;
        run.version_ref_pinned = true;
    }
    pub fn takeVersionPin(run: *Run) ?*pins.Registry.Pin {
        const owner = run.owner orelse return null;
        if (owner.destroy != Payload.destroy or owner.raw != run) return null;
        const payload: *Payload = @alignCast(@fieldParentPtr("owner", owner));
        const pin = payload.version_pin;
        payload.version_pin = null;
        if (pin != null) run.version_ref_pinned = false;
        return pin;
    }
    pub const empty: Store = .{};
    tree: Tree = .{},
    destroy_run: ?DestroyRun = null,
    retired_next: ?*Store = null,
    registered_owner: ?*@import("../resource_manager.zig").PayloadRetirement = null,
    registered_account: ?*Account = null,
    // Explicit test/oracle projection. No production writer reads or maintains
    // this vector; structural edits invalidate it, like any borrowed cursor.
    test_projection: if (@import("builtin").is_test) ?[]Run else void = if (@import("builtin").is_test) null else {},

    pub fn count(self: *const Store) usize {
        return if (self.tree.root) |root| root.count else 0;
    }
    pub fn at(self: *const Store, rank: usize) *Run {
        return self.tree.root.?.at(rank).run;
    }
    pub fn find(self: *const Store, run: *const Run) ?*Run {
        return if (Tree.find(self.tree.root, .{ .run = @constCast(run) })) |node| node.entry.run else null;
    }
    pub fn rankOf(self: *const Store, run: *const Run) ?usize {
        if (self.find(run) == null) return null;
        return self.tree.root.?.lowerBound(.{ .run = @constCast(run) });
    }

    /// Only immutable fields are read from a pinned writer root. Mutable
    /// cache hints may be changing concurrently and belong to the new record.
    pub fn revision(source: *const Run, metadata: Run) Run {
        var run = metadata;
        run.read_identity = source.read_identity;
        run.owner = source.owner;
        run.path = source.path;
        run.smallest_namespace_name = source.smallest_namespace_name;
        run.smallest_key = source.smallest_key;
        run.largest_namespace_name = source.largest_namespace_name;
        run.largest_key = source.largest_key;
        run.state = if (source.path == null) source.state else null;
        run.bloom_filter = null;
        run.owns_bloom_filter = false;
        run.table_index = null;
        run.version_ref_pinned = false;
        return run;
    }
    pub fn levelStart(self: *const Store, level: u32) usize {
        var root = self.tree.root;
        var rank: usize = 0;
        while (root) |node| {
            if (node.entry.run.level < level) {
                rank += 1 + (if (node.left) |left| left.count else 0);
                root = node.right;
            } else root = node.left;
        }
        return rank;
    }
    pub fn countLevel(self: *const Store, level: u32) usize {
        return (if (level == std.math.maxInt(u32)) self.count() else self.levelStart(level + 1)) - self.levelStart(level);
    }
    pub fn maxLevel(self: *const Store) u32 {
        return if (self.count() == 0) 0 else self.at(self.count() - 1).level;
    }
    pub fn totalBytes(self: *const Store) u64 {
        return if (self.tree.root) |root| root.summary.run_bytes else 0;
    }
    pub fn l0Files(self: *const Store) usize {
        return if (self.tree.root) |root| root.summary.l0_files else 0;
    }
    pub fn l0Bytes(self: *const Store) u64 {
        return if (self.tree.root) |root| root.summary.l0_bytes else 0;
    }
    pub fn manifestBytes(self: *const Store) u64 {
        return if (self.tree.root) |root| root.summary.manifest_bytes else 0;
    }
    pub fn fork(self: *const Store) Store {
        return .{ .tree = self.tree.fork(), .destroy_run = self.destroy_run, .registered_owner = self.registered_owner, .registered_account = if (self.registered_account) |account| account.retain() else null };
    }
    pub fn emptyLike(self: *const Store) Store {
        return .{ .tree = .{ .account = if (self.tree.account) |account| account.retain() else null }, .destroy_run = self.destroy_run, .registered_owner = self.registered_owner, .registered_account = if (self.registered_account) |account| account.retain() else null };
    }
    pub fn deinit(self: *Store, allocator: std.mem.Allocator) void {
        self.invalidateProjection(allocator);
        const guard = self.registered_account;
        self.tree.deinit(allocator);
        self.* = .{ .destroy_run = self.destroy_run };
        if (guard) |account| account.release();
    }
    pub const Reclaimer = struct {
        store: *Store,
        tree: Tree.Reclaimer,
        pub fn init(store: *Store) @This() {
            if (store.tree.account) |account| _ = account.retain();
            return .{ .store = store, .tree = .init(store.tree) };
        }
        pub fn step(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
            return self.tree.step(allocator, credits);
        }
        pub fn stepWithAdmission(self: *@This(), allocator: std.mem.Allocator, credits: *usize, original: ?admission.Port) !bool {
            if (original) |port| try port.checkpoint();
            if (comptime @import("builtin").is_test) if (original != null and self.store.test_projection != null) return error.ExecutionWaitUnsupported;
            return self.tree.stepWithAdmission(allocator, credits, original);
        }
        pub fn finish(self: *@This(), allocator: std.mem.Allocator) void {
            self.store.invalidateProjection(allocator);
            if (self.store.tree.account) |account| account.release();
            const guard = self.store.registered_account;
            allocator.destroy(self.store);
            if (guard) |account| account.release();
        }
        /// Value snapshots have caller-funded headers and no diagnostic vector.
        pub fn finishContents(self: *@This()) void {
            if (comptime @import("builtin").is_test) std.debug.assert(self.store.test_projection == null);
            const guard = self.store.registered_account;
            if (self.store.tree.account) |account| account.release();
            if (guard) |account| account.release();
        }
    };
    /// Registered root/header creators already reserve this allocation and
    /// retain its funding through final header destruction.
    pub fn observedHeaderBytes(self: *const Store) u64 {
        return if (self.registered_owner != null) 0 else @sizeOf(Store);
    }
    pub fn memoryBytes(self: *const Store, pass: u64) u64 {
        if (self.registered_owner != null) return 0;
        return self.tree.spare.capacity * @sizeOf(*Tree.Node) +
            (if (self.tree.account) |account| account.chargeOnce(pass) else 0);
    }
    pub const Cursor = struct {
        store: *const Store,
        index: Tree.Cursor = .{},
        rank: usize = 0,
        pub fn next(self: *@This()) ?*Run {
            if (self.rank == self.store.count()) return null;
            defer self.rank += 1;
            return self.index.at(self.store.tree.root.?, self.rank).run;
        }
    };
    pub fn cursor(self: *const Store) Cursor {
        return .{ .store = self };
    }

    pub fn project(self: *const Store, allocator: std.mem.Allocator) ![]Run {
        const runs = try allocator.alloc(Run, self.count());
        var iterator = self.cursor();
        for (runs) |*run| run.* = iterator.next().?.*;
        return runs;
    }
    pub fn testItems(self: *Store, allocator: std.mem.Allocator) ![]const Run {
        if (!@import("builtin").is_test) @compileError("writer projections are test/oracle only");
        if (self.test_projection == null) self.test_projection = try self.project(allocator);
        return self.test_projection.?;
    }
    fn invalidateProjection(self: *Store, allocator: std.mem.Allocator) void {
        if (@import("builtin").is_test) {
            if (self.test_projection) |projection| allocator.free(projection);
            self.test_projection = null;
        }
    }

    /// Staged payloads borrow incoming ownership. Aborting a prepared root
    /// frees its tree/payloads but leaves the caller's SST metadata intact.
    pub fn stage(self: *Store, allocator: std.mem.Allocator, run: Run) !void {
        return self.stageInternal(allocator, run, false);
    }
    pub fn stageRevision(self: *Store, allocator: std.mem.Allocator, run: Run) !void {
        return self.stageInternal(allocator, run, true);
    }
    fn stageInternal(self: *Store, allocator: std.mem.Allocator, run: Run, overwrite: bool) !void {
        if (self.registered_owner != null) return error.ExecutionWaitUnsupported;
        if (run.owner) |owner| if (owner.registered != null) {
            // A completed native output may retain its registered physical
            // owner behind this legacy projection. Every other registered
            // record still requires the registered candidate protocol.
            try @import("registered_writer.zig").Record.validateLegacyOutputFixed(&run);
        };
        var probe = run;
        if (!overwrite and Tree.find(self.tree.root, .{ .run = &probe }) != null) return error.DuplicateRun;
        try probe.ensureReadIdentity();
        try self.tree.prepare(allocator);
        const payload = try allocator.create(Payload);
        const bytes = @sizeOf(Payload) + if (run.owner != null) @as(usize, 0) else run.smallest_key.len + run.largest_key.len +
            (if (run.path) |path| path.len else 0) +
            (if (run.smallest_namespace_name) |name| name.len else 0) +
            (if (run.largest_namespace_name) |name| name.len else 0) +
            (if (run.state) |present| present.estimatedMemoryBytes() else 0);
        self.tree.account.?.charge(bytes);
        const parent = if (run.owner) |owner| owner.raw.owner.?.retain() else null;
        payload.* = .{ .owner = .{ .raw = if (parent) |owner| owner.raw else &payload.run, .destroy = Payload.destroy }, .parent = parent, .run = probe, .destroy_run = self.destroy_run, .account = self.tree.account.?, .bytes = bytes };
        payload.run.owner = &payload.owner;
        if (parent != null) {
            payload.run.bloom_filter = null;
            payload.run.owns_bloom_filter = false;
            payload.run.table_index = null;
            payload.run.cached_state_index = null;
            payload.run.cached_index_index = null;
            payload.run.cached_table_index = null;
            payload.run.version_ref_pinned = false;
            if (payload.run.path != null) payload.run.state = null;
        }
        const entry = Entry{ .run = &payload.run, .payload = payload };
        defer entry.deinit(allocator);
        self.tree.putPrepared(allocator, entry);
        self.invalidateProjection(allocator);
    }
    /// Controlled insertion of a completed registered output into a private
    /// legacy writer candidate. The caller prepays the fixed wrapper/pool and
    /// retains the entire candidate through abort; every path copy stays rooted.
    pub const OutputInsertion = struct {
        store: *Store,
        payload: ?*Payload = null,
        low: usize = 0,
        high: usize = 0,
        middle: usize = 0,
        comparison: ?@import("run_comparison.zig").Cursor = null,
        rank: ?usize = null,
        edit: ?Tree.RankInsertion = null,
        complete: bool = false,

        pub fn memoryBound(store: *const Store) !u64 {
            return std.math.add(u64, @sizeOf(Payload), try store.tree.prepareMemoryBound(1));
        }

        pub fn init(store: *Store, allocator: std.mem.Allocator, run: *const Run, original: admission.Port) !@This() {
            try original.checkpoint();
            if (store.registered_owner != null or store.testProjectionPresent()) return error.ExecutionWaitUnsupported;
            try @import("registered_writer.zig").Record.validateLegacyOutputFixed(run);
            try store.tree.prepareWithAdmission(allocator, original);
            const payload = try allocator.create(Payload);
            const parent = run.owner.?.raw.owner.?.retain();
            store.tree.account.?.charge(@sizeOf(Payload));
            payload.* = .{ .owner = .{ .raw = parent.raw, .destroy = Payload.destroy }, .parent = parent, .run = run.*, .owned = true, .destroy_run = store.destroy_run, .account = store.tree.account.?, .bytes = @sizeOf(Payload) };
            payload.run.owner = &payload.owner;
            payload.run.bloom_filter = null;
            payload.run.owns_bloom_filter = false;
            payload.run.table_index = null;
            payload.run.cached_state_index = null;
            payload.run.cached_index_index = null;
            payload.run.cached_table_index = null;
            payload.run.version_ref_pinned = false;
            if (payload.run.path != null) payload.run.state = null;
            return .{ .store = store, .payload = payload, .high = store.count() };
        }
        pub fn resumeWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: admission.Port) !void {
            try original.checkpoint();
            if (self.complete) return;
            const payload = self.payload orelse return error.InvalidArgument;
            while (self.rank == null) {
                try original.checkpoint();
                if (self.low == self.high) {
                    self.rank = self.low;
                    break;
                }
                if (self.comparison == null) {
                    self.middle = self.low + (self.high - self.low) / 2;
                    self.comparison = .init(.read, .{ .run = self.store.at(self.middle) }, .{ .run = &payload.run });
                }
                const order = try self.comparison.?.resumeWithAdmission(original);
                self.comparison = null;
                switch (order) {
                    .lt => self.low = self.middle + 1,
                    .gt => self.high = self.middle,
                    .eq => return error.DuplicateRun,
                }
            }
            if (self.edit == null) self.edit = try Tree.RankInsertion.init(&self.store.tree, .{ .run = &payload.run, .payload = payload }, self.rank.?, false);
            try self.edit.?.resumeWithAdmission(allocator, original);
            std.debug.assert(try self.edit.?.takeRetired() == null);
            self.complete = true;
        }
        pub fn takeWithAdmission(self: *@This(), original: admission.Port) !Run {
            try original.checkpoint();
            if (!self.complete or self.payload == null) return error.InvalidArgument;
            return self.payload.?.run.retainOwned();
        }
        /// Final fixed drop only: the source is registered, caches are empty,
        /// and any partially inserted path remains owned by the private tree.
        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (self.payload) |payload| payload.owner.release(allocator);
            self.payload = null;
        }
    };

    fn testProjectionPresent(self: *const Store) bool {
        return if (comptime @import("builtin").is_test) self.test_projection != null else false;
    }

    /// Registered candidates prepay the pool and prove this exact rank with
    /// bounded comparisons. The caller pins run through completion/abort.
    pub fn beginRegisteredInsertion(self: *Store, run: Run, rank: usize, replacing: bool, original: @import("../../common/read_admission.zig").Port) !Tree.RankInsertion {
        try original.checkpoint();
        if (self.registered_owner == null) return error.ExecutionWaitUnsupported;
        const record = try @import("registered_writer.zig").Record.validate(&run, original);
        if (replacing and rank < self.count()) _ = try @import("registered_writer.zig").Record.validate(self.at(rank), original);
        return Tree.RankInsertion.init(&self.tree, .{ .run = &record.run }, rank, replacing);
    }
    pub fn adopt(self: *Store, run: *const Run) void {
        const payload = Tree.find(self.tree.root, .{ .run = @constCast(run) }).?.entry.payload.?;
        std.debug.assert(!payload.owned);
        payload.owned = true;
    }
    pub fn append(self: *Store, allocator: std.mem.Allocator, run: Run) !void {
        try self.stage(allocator, run);
        self.adopt(&run);
        if (run.owner) |owner| owner.release(allocator);
    }
    pub fn reindexForTest(self: *Store, allocator: std.mem.Allocator) !void {
        if (!@import("builtin").is_test) @compileError("fixture metadata edits require test mode");
        var rebuilt = self.emptyLike();
        errdefer rebuilt.deinit(allocator);
        var source = self.cursor();
        while (source.next()) |run| try rebuilt.stage(allocator, run.*);
        source = self.cursor();
        while (source.next()) |run| {
            rebuilt.adopt(run);
        }
        std.mem.swap(Store, self, &rebuilt);
        rebuilt.deinit(allocator);
    }
    pub fn remove(self: *Store, allocator: std.mem.Allocator, run: *const Run) !void {
        if (self.registered_owner != null) return error.ExecutionWaitUnsupported;
        try self.tree.prepare(allocator);
        self.tree.removePrepared(allocator, .{ .run = @constCast(run) });
        self.invalidateProjection(allocator);
    }
    pub fn replace(self: *Store, allocator: std.mem.Allocator, old: *Run, replacement: Run) !void {
        var candidate = try self.prepareReplace(allocator, old, replacement);
        std.mem.swap(Store, self, &candidate);
        candidate.deinit(allocator);
    }
    pub fn prepareReplace(self: *const Store, allocator: std.mem.Allocator, old: *Run, replacement: Run) !Store {
        var candidate = self.fork();
        errdefer candidate.deinit(allocator);
        try candidate.remove(allocator, old);
        try candidate.stage(allocator, replacement);
        candidate.adopt(&replacement);
        return candidate;
    }
};

test "writer owner narrow publication scaling benchmark" {
    if (@import("builtin").mode != .fast) return error.SkipZigTest;
    const allocator = std.heap.smp_allocator;
    const clock = @import("antfly_platform").time;
    for ([_]usize{ 1000, 10000, 100000 }) |count_runs| {
        var live: Store = .{};
        defer live.deinit(allocator);
        const template = Run{ .id = 0, .level = 0, .size_bytes = 1, .path = null, .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 1, .bloom_filter = null, .state = null };
        for (0..count_runs) |i| {
            var run = template;
            run.id = i + 1;
            try live.append(allocator, run);
        }
        const baseline = try live.project(allocator);
        defer allocator.free(baseline);
        var tree_ns: u64 = 0;
        var array_ns: u64 = 0;
        for (0..31) |_| {
            const array_start = clock.monotonicNs();
            const array = try allocator.alloc(Run, count_runs - 3);
            @memcpy(array[0 .. count_runs - 4], baseline[4..]);
            var output = template;
            output.id = count_runs + 1;
            array[array.len - 1] = output;
            @import("compaction.zig").sortRuns(array);
            std.mem.doNotOptimizeAway(array[0].id);
            allocator.free(array);
            array_ns += clock.monotonicNs() - array_start;
            const tree_start = clock.monotonicNs();
            var candidate = live.fork();
            for (0..4) |i| try candidate.remove(allocator, live.at(i));
            try candidate.append(allocator, output);
            std.mem.doNotOptimizeAway(candidate.at(0).id);
            candidate.deinit(allocator);
            tree_ns += clock.monotonicNs() - tree_start;
        }
        std.debug.print("writer-owner runs={d} changed=5 array_mean_ns={d} tree_mean_ns={d} retained_bytes={d}\n", .{ count_runs, array_ns / 31, tree_ns / 31, live.memoryBytes(@import("memory_account.zig").nextPass()) });
    }
}

test "lsm local decoder admission immutable identity survives revisions and snapshots" {
    const a = std.testing.allocator;
    var run: Run = .{ .id = 1, .level = 0, .size_bytes = 64, .path = try a.dupe(u8, "same-path"), .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 1, .bloom_filter = null, .state = null };
    var adopted = false;
    defer if (!adopted) run.deinit(a);
    var store: Store = .{};
    defer store.deinit(a);
    try store.append(a, run);
    adopted = true;
    const identity = store.at(0).read_identity;
    try std.testing.expect(identity.valid());
    var snapshot = try @import("repository.zig").cloneRunSnapshot(a, store.at(0).*);
    defer snapshot.deinit(a);
    var compact = try @import("repository.zig").cloneRunCompactionSnapshot(a, store.at(0).*);
    defer compact.deinit(a);
    try std.testing.expect(std.meta.eql(identity, snapshot.read_identity));
    try std.testing.expect(std.meta.eql(identity, compact.read_identity));
    var metadata = store.at(0).*;
    metadata.level = 1;
    metadata.gc_requested = true;
    // A stale logical plan cannot replace the pinned physical source's identity.
    metadata.read_identity = try @import("read_generation.zig").allocate();
    const revised = Store.revision(store.at(0), metadata);
    try std.testing.expect(std.meta.eql(identity, revised.read_identity));
    var candidate = store.fork();
    defer candidate.deinit(a);
    try candidate.remove(a, store.at(0));
    try candidate.stageRevision(a, revised);
    candidate.adopt(&revised);
    try std.testing.expect(std.meta.eql(identity, candidate.at(0).read_identity));
    var replacement = try @import("repository.zig").cloneRunCompactionSnapshot(a, revised);
    replacement.read_identity = .{};
    var replacement_owned = true;
    defer if (replacement_owned) replacement.deinit(a);
    var fresh: Store = .{};
    defer fresh.deinit(a);
    try fresh.append(a, replacement);
    replacement_owned = false;
    try std.testing.expect(!std.meta.eql(identity, fresh.at(0).read_identity));
}
