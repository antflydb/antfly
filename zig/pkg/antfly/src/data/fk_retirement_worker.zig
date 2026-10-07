// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Physical retirement of an exact terminal obsolete initial-FK replica.
//! The prepared ticket survives process death before/after rename. A receipt
//! is created only after both rename parents and the unlinked intent are synced.
const platform = @import("antfly_platform");
const std = @import("std");

const fs = @import("antfly_runtime_fs").fs_paths;
const contract = @import("../metadata/fk_initial_retirement_contract.zig");
const auth = @import("../metadata/fk_initial_retirement_auth.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;
const publication = @import("antfly_local_sources").storage_db_relational_initial_child_publication;

pub const Ticket = contract.Ticket;
pub const journal_name = ".antfly-initial-fk-retirement";
pub const completed_name = ".antfly-initial-fk-retired";
pub const trash_name = ".antfly-initial-fk-trash";
pub const ReplicaIdentity = struct {
    group_id: u64,
    replica_id: u64,
    node_id: u64,
    root_generation: u64,
};
pub const Observation = struct {
    replica: ?ReplicaIdentity,
    publication: ?publication.Record,
};
pub const Action = struct {
    ptr: *anyopaque,
    run: *const fn (*anyopaque) anyerror!void,
};
pub const Callbacks = struct {
    ptr: *anyopaque,
    /// Reserve the exact group against placement/provisioning. Must remain
    /// exclusive until release, including on failure/retry. Raft keeps running
    /// until the exact cold hidden/canceled AICH has been proven.
    acquire: *const fn (*anyopaque, Ticket) anyerror!void,
    release: *const fn (*anyopaque, Ticket) void,
    /// Read the actual cold AICH at group_path and durable local catalog.
    /// Never synthesize absent/canceled publication from metadata intent.
    observe: *const fn (*anyopaque, Ticket, []const u8) anyerror!Observation,
    /// Called only after owner drain and an exact hidden AICH check. The
    /// authenticated terminal ticket authorizes local cancellation without a
    /// departed Raft quorum, never public release or a logical owner receipt.
    cancel_hidden: ?*const fn (*anyopaque, Ticket, []const u8, publication.Record) anyerror!void = null,
    /// Stop/drain Raft and writers/readers/background owners; run action while
    /// exclusion is still held (table_writes.withQuiescedInitialFkReplica).
    quiesce: *const fn (*anyopaque, Ticket, Action) anyerror!void,
    /// Compare the entire local identity before removal; preserve a replacement.
    /// Must durably remove the exact old record before returning success.
    remove_catalog_exact: *const fn (*anyopaque, Ticket) anyerror!void,
    acknowledge: *const fn (*anyopaque, auth.SignedReceipt) anyerror!void,
};

pub const FaultPoint = enum { prepared, canceled, renamed, parents_synced, unlinked, catalog_removed, acknowledged };

/// Retains only a directory handle/cursor, not a list proportional to history.
/// A failed first item cannot monopolize bounded discovery across ticks.
pub const RecoveryCursor = struct {
    dir: ?std.Io.Dir = null,
    io: ?std.Io = null,
    iterator: std.Io.Dir.Iterator = undefined,

    pub fn deinit(self: *@This()) void {
        if (self.dir) |dir| dir.close(self.io.?);
        self.* = .{};
    }
    fn ensure(self: *@This(), io: std.Io, path: []const u8) !bool {
        if (self.dir) |dir| {
            const current = std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false }) catch |err| switch (err) {
                error.FileNotFound => {
                    self.deinit();
                    return false;
                },
                else => return err,
            };
            if (current.kind == .directory and current.inode == (try dir.stat(io)).inode) return true;
            self.deinit();
        }
        const dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        self.dir = dir;
        self.io = io;
        self.iterator = dir.iterate();
        return true;
    }
};

pub const Worker = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    node_id: u64,
    store_id: u64,
    reporter_incarnation: u64,
    metadata_incarnation: [32]u8,
    root: signing.State,
    callbacks: Callbacks,
    continue_work: ?struct { ptr: *anyopaque, check: *const fn (*anyopaque) bool } = null,
    recovery_cursor: ?*RecoveryCursor = null,
    gc_cursor: ?*RecoveryCursor = null,
    /// Deterministic crash-boundary injection, never used for production policy.
    fault: ?struct { ptr: *anyopaque, hit: *const fn (*anyopaque, FaultPoint) anyerror!void } = null,

    fn hit(self: *Worker, point: FaultPoint) !void {
        if (self.fault) |fault| try fault.hit(fault.ptr, point);
    }

    pub fn process(self: *Worker, ticket: Ticket) !void {
        if (!self.canContinue()) return error.InitialFkRetirementSliceExpired;
        try ticket.validate();
        if (ticket.replica.node_id != self.node_id or ticket.replica.store_id != self.store_id or
            ticket.replica.store_root_incarnation != self.root.root_incarnation or self.reporter_incarnation == 0 or
            !std.mem.eql(u8, &ticket.metadata_incarnation, &self.metadata_incarnation))
            return error.InitialFkRetirementRootChanged;
        try self.callbacks.acquire(self.callbacks.ptr, ticket);
        var acquired = true;
        defer if (acquired) self.callbacks.release(self.callbacks.ptr, ticket);

        var paths = try Paths.init(self.alloc, self.replica_root_dir, ticket);
        defer paths.deinit(self.alloc);
        try ensureDirectory(self.io, paths.journal_dir);
        try ensureDirectory(self.io, paths.trash_dir);
        try ensureDirectory(self.io, paths.done_dir);
        // Newly created queue/trash directory names must survive a root crash.
        try fs.syncDirPortable(self.io, self.replica_root_dir);

        var intent = (try self.load(paths.intent_path)) orelse try self.load(paths.done_path);
        if (intent) |value| {
            if (!std.meta.eql(value.ticket, ticket)) return error.InitialFkRetirementWorkChanged;
        } else {
            if (!try isDirectory(self.io, paths.group_path)) return error.InitialFkRetirementProofUnavailable;
            try requireExactPhase(ticket, try self.callbacks.observe(self.callbacks.ptr, ticket, paths.group_path), self.callbacks.cancel_hidden != null);
            intent = .{ .ticket = ticket, .phase = .prepared };
            try self.persist(paths, intent.?);
            try self.hit(.prepared);
        }
        if (intent.?.phase == .prepared) {
            var continuation: Continuation = .{ .worker = self, .paths = paths, .ticket = ticket };
            try self.callbacks.quiesce(self.callbacks.ptr, ticket, .{ .ptr = &continuation, .run = Continuation.run });
            // A callback must actually execute the protected continuation.
            if (!continuation.completed) return error.InitialFkRetirementNotUnlinked;
            intent = .{ .ticket = ticket, .phase = .unlinked };
        }
        try self.callbacks.remove_catalog_exact(self.callbacks.ptr, ticket);
        try self.hit(.catalog_removed);
        // Network ACK never holds the placement/physical-owner exclusion.
        self.callbacks.release(self.callbacks.ptr, ticket);
        acquired = false;
        if (!self.canContinue()) return error.InitialFkRetirementSliceExpired;
        const receipt = try contract.Receipt.fromUnlinkedIntent(intent.?, self.reporter_incarnation);
        try self.callbacks.acknowledge(self.callbacks.ptr, try auth.SignedReceipt.sign(self.root, receipt));
        try self.hit(.acknowledged);
        // O(1) proof lookup when enumerating actual trash. Publish this before
        // removing pending work, so a crash cannot strand unindexed garbage.
        if (try isDirectory(self.io, paths.trash_path)) {
            const gc_path = try std.fmt.allocPrint(self.alloc, "{s}/{s}.gc", .{ paths.done_dir, std.fs.path.basename(paths.trash_path) });
            defer self.alloc.free(gc_path);
            const stat = try std.Io.Dir.cwd().statFile(self.io, paths.trash_path, .{ .follow_symlinks = false });
            const proof: GcProof = .{ .intent = intent.?, .inode = @intCast(stat.inode) };
            const bytes = try std.json.Stringify.valueAlloc(self.alloc, proof, .{});
            defer self.alloc.free(bytes);
            // A repeated old page must not reauthorize a replaced trash root.
            var done_dir = try std.Io.Dir.cwd().openDir(self.io, paths.done_dir, .{ .follow_symlinks = false });
            defer done_dir.close(self.io);
            if (try loadGcFromDir(self.alloc, self.io, done_dir, std.fs.path.basename(gc_path))) |old| {
                if (!std.meta.eql(old, proof)) return error.InitialFkRetirementPathChanged;
            } else try self.persistBytes(gc_path, paths.done_dir, bytes);
        }
        // Retain a tombstone so stale placement cannot rehost this generation.
        std.Io.Dir.rename(std.Io.Dir.cwd(), paths.intent_path, std.Io.Dir.cwd(), paths.done_path, self.io) catch |err| switch (err) {
            error.FileNotFound => if ((try self.load(paths.done_path)) == null) return err,
            else => return err,
        };
        try fs.syncDirPortable(self.io, paths.journal_dir);
        try fs.syncDirPortable(self.io, paths.done_dir);
        // Bounded collectTrash slices reclaim bytes separately. ACK proves
        // durable logical unlink, never an unbounded recursive deletion.
    }

    /// Bounded local discovery also retries ACKs whose metadata page disappeared
    /// after a successful ACK response was lost. Every file self-identifies.
    pub fn recover(self: *Worker, limit: usize) !usize {
        const directory = try std.fs.path.join(self.alloc, &.{ self.replica_root_dir, journal_name });
        defer self.alloc.free(directory);
        var local_cursor: RecoveryCursor = .{};
        defer local_cursor.deinit();
        const cursor = self.recovery_cursor orelse &local_cursor;
        if (!try cursor.ensure(self.io, directory)) return 0;
        var count: usize = 0;
        var scanned: usize = 0;
        while (scanned < limit and self.canContinue()) {
            const entry = (try cursor.iterator.next(self.io)) orelse {
                cursor.deinit();
                break;
            };
            scanned += 1;
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".bin")) continue;
            const path = try std.fs.path.join(self.alloc, &.{ directory, entry.name });
            defer self.alloc.free(path);
            const intent = (self.load(path) catch continue) orelse continue;
            var expected = try Paths.init(self.alloc, self.replica_root_dir, intent.ticket);
            defer expected.deinit(self.alloc);
            if (!std.mem.eql(u8, path, expected.intent_path)) continue;
            self.process(intent.ticket) catch |err| {
                std.log.warn("initial FK retirement queued retry deferred group={d} err={s}", .{ intent.ticket.replica.group_id, @errorName(err) });
                continue;
            };
            count += 1;
        }
        return count;
    }

    fn canContinue(self: *Worker) bool {
        return if (self.continue_work) |callback| callback.check(callback.ptr) else true;
    }

    fn load(self: *Worker, path: []const u8) !?contract.Intent {
        const bytes = std.Io.Dir.cwd().readFileAlloc(self.io, path, self.alloc, .limited(contract.intent_encoded_len + 1)) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer self.alloc.free(bytes);
        return try contract.Intent.decode(bytes);
    }

    fn persist(self: *Worker, paths: Paths, intent: contract.Intent) !void {
        return self.persistAt(paths.intent_path, paths.journal_dir, intent);
    }

    fn persistAt(self: *Worker, path: []const u8, directory: []const u8, intent: contract.Intent) !void {
        const bytes = try intent.encode();
        return self.persistBytes(path, directory, &bytes);
    }

    fn persistBytes(self: *Worker, path: []const u8, directory: []const u8, bytes: []const u8) !void {
        const temp = try std.fmt.allocPrint(self.alloc, "{s}.tmp", .{path});
        defer self.alloc.free(temp);
        defer std.Io.Dir.cwd().deleteFile(self.io, temp) catch {};
        std.Io.Dir.cwd().deleteFile(self.io, temp) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        {
            var file = try fs.createFilePortable(self.io, temp, .{ .exclusive = true });
            defer file.close(self.io);
            var buffer: [contract.intent_encoded_len]u8 = undefined;
            var writer = file.writer(self.io, &buffer);
            try writer.interface.writeAll(bytes);
            try writer.end();
            try file.sync(self.io);
        }
        try std.Io.Dir.rename(std.Io.Dir.cwd(), temp, std.Io.Dir.cwd(), path, self.io);
        try fs.syncDirPortable(self.io, directory);
    }

    pub const GcBudget = struct { max_entries: usize = 256, max_deletes: usize = 128, max_depth: usize = 32, max_ns: u64 = 20 * std.time.ns_per_ms };
    pub const GcResult = struct { examined: usize = 0, deleted: usize = 0, roots: usize = 0 };
    const GcSlice = struct {
        worker: *Worker,
        budget: GcBudget,
        deadline: i96,
        result: GcResult = .{},
        fn available(self: *@This()) bool {
            return self.worker.canContinue() and self.result.examined < self.budget.max_entries and self.result.deleted < self.budget.max_deletes and std.Io.Clock.awake.now(self.worker.io).toNanoseconds() < self.deadline;
        }
        // Operations remain relative to opened no-follow directory handles;
        // symlinks are unlinked as leaves and are never traversed.
        fn prune(self: *@This(), dir: std.Io.Dir, depth: usize) !bool {
            if (depth >= self.budget.max_depth) return false;
            var iterator = dir.iterate();
            while (self.available()) {
                const entry = (try iterator.next(self.worker.io)) orelse return true;
                self.result.examined += 1;
                const stat = dir.statFile(self.worker.io, entry.name, .{ .follow_symlinks = false }) catch |err| switch (err) {
                    error.FileNotFound => continue,
                    else => return err,
                };
                if (stat.kind == .directory) {
                    var child = try dir.openDir(self.worker.io, entry.name, .{ .iterate = true, .follow_symlinks = false });
                    defer child.close(self.worker.io);
                    if ((try child.stat(self.worker.io)).inode != stat.inode) return false;
                    if (!try self.prune(child, depth + 1)) return false;
                    if (!self.available()) return false;
                    if ((try dir.statFile(self.worker.io, entry.name, .{ .follow_symlinks = false })).inode != stat.inode) return false;
                    dir.deleteDir(self.worker.io, entry.name) catch |err| switch (err) {
                        error.FileNotFound => continue,
                        error.DirNotEmpty => return false,
                        else => return err,
                    };
                } else {
                    dir.deleteFile(self.worker.io, entry.name) catch |err| switch (err) {
                        error.FileNotFound => continue,
                        else => return err,
                    };
                }
                self.result.deleted += 1;
            }
            return false;
        }
    };

    /// Enumerates only trash, never the growing permanent tombstone catalog.
    /// A ticket-digest index and its exact ACKed .done intent are both required.
    /// Every restart can safely resume from the remaining directory contents.
    pub fn collectTrash(self: *Worker, budget: GcBudget) !GcResult {
        var root = try std.Io.Dir.cwd().openDir(self.io, self.replica_root_dir, .{ .follow_symlinks = false });
        defer root.close(self.io);
        var trash = root.openDir(self.io, trash_name, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => return err,
        };
        defer trash.close(self.io);
        var done = root.openDir(self.io, completed_name, .{ .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => return err,
        };
        defer done.close(self.io);
        var slice: GcSlice = .{ .worker = self, .budget = budget, .deadline = std.Io.Clock.awake.now(self.io).toNanoseconds() +| budget.max_ns };
        var local_cursor: RecoveryCursor = .{};
        defer local_cursor.deinit();
        const cursor = self.gc_cursor orelse &local_cursor;
        const trash_path = try std.fs.path.join(self.alloc, &.{ self.replica_root_dir, trash_name });
        defer self.alloc.free(trash_path);
        if (!try cursor.ensure(self.io, trash_path)) return .{};
        if ((try cursor.dir.?.stat(self.io)).inode != (try trash.stat(self.io)).inode) return error.InitialFkRetirementPathChanged;
        while (slice.available()) {
            const entry = (try cursor.iterator.next(self.io)) orelse {
                cursor.deinit();
                break;
            };
            slice.result.examined += 1;
            if (entry.kind != .directory or entry.name.len != 64) continue;
            var gc_name_buf: [67]u8 = undefined;
            const gc_name = try std.fmt.bufPrint(&gc_name_buf, "{s}.gc", .{entry.name});
            const indexed = (try loadGcFromDir(self.alloc, self.io, done, gc_name)) orelse continue;
            if (indexed.intent.phase != .unlinked) continue;
            const ticket = indexed.intent.ticket;
            const digest = std.fmt.bytesToHex(try ticket.digest(), .lower);
            if (!std.mem.eql(u8, entry.name, &digest) or ticket.replica.node_id != self.node_id or ticket.replica.store_id != self.store_id or
                ticket.replica.store_root_incarnation != self.root.root_incarnation or !std.mem.eql(u8, &ticket.metadata_incarnation, &self.metadata_incarnation)) continue;
            var done_name_buf: [128]u8 = undefined;
            const done_name = try std.fmt.bufPrint(&done_name_buf, "group-{d}-replica-{d}-generation-{d}.done", .{ ticket.replica.group_id, ticket.replica.replica_id, ticket.replica.root_generation });
            const proof = (try loadFromDir(self.alloc, self.io, done, done_name)) orelse continue;
            if (!std.meta.eql(indexed.intent, proof)) continue;
            var child = try trash.openDir(self.io, entry.name, .{ .iterate = true, .follow_symlinks = false });
            defer child.close(self.io);
            if ((try child.stat(self.io)).inode != indexed.inode) continue;
            if (!try slice.prune(child, 0) or !slice.available()) break;
            if ((try trash.statFile(self.io, entry.name, .{ .follow_symlinks = false })).inode != indexed.inode) continue;
            trash.deleteDir(self.io, entry.name) catch |err| switch (err) {
                error.FileNotFound => {},
                error.DirNotEmpty => continue,
                else => return err,
            };
            slice.result.deleted += 1;
            slice.result.roots += 1;
            // Retire the inode authorization only after the empty root's
            // unlink is durable. The permanent .done resurrection fence stays.
            try fs.syncDirPortable(self.io, trash_path);
            done.deleteFile(self.io, gc_name) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
            const done_path = try std.fs.path.join(self.alloc, &.{ self.replica_root_dir, completed_name });
            defer self.alloc.free(done_path);
            try fs.syncDirPortable(self.io, done_path);
        }
        if (slice.result.deleted != 0) {
            const path = try std.fs.path.join(self.alloc, &.{ self.replica_root_dir, trash_name });
            defer self.alloc.free(path);
            try fs.syncDirPortable(self.io, path);
        }
        return slice.result;
    }

    const Continuation = struct {
        worker: *Worker,
        paths: Paths,
        ticket: Ticket,
        completed: bool = false,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const worker = self.worker;
            const source_exists = try isDirectory(worker.io, self.paths.group_path);
            const trash_exists = try isDirectory(worker.io, self.paths.trash_path);
            if (source_exists == trash_exists) return error.InitialFkRetirementPathChanged;
            // Both fresh retirement and a crash after rename repeat cold AICH
            // and catalog validation while every physical owner is quiesced.
            const path = if (source_exists) self.paths.group_path else self.paths.trash_path;
            const observed = try worker.callbacks.observe(worker.callbacks.ptr, self.ticket, path);
            try requireExactPhase(self.ticket, observed, worker.callbacks.cancel_hidden != null);
            if (observed.publication.?.phase == .hidden) {
                try worker.callbacks.cancel_hidden.?(worker.callbacks.ptr, self.ticket, path, observed.publication.?);
            }
            try requireExact(self.ticket, try worker.callbacks.observe(worker.callbacks.ptr, self.ticket, path));
            try worker.hit(.canceled);
            if (source_exists) {
                try std.Io.Dir.rename(std.Io.Dir.cwd(), self.paths.group_path, std.Io.Dir.cwd(), self.paths.trash_path, worker.io);
                try worker.hit(.renamed);
            }
            try fs.syncDirPortable(worker.io, worker.replica_root_dir);
            try fs.syncDirPortable(worker.io, self.paths.trash_dir);
            try worker.hit(.parents_synced);
            try worker.persist(self.paths, .{ .ticket = self.ticket, .phase = .unlinked });
            try worker.hit(.unlinked);
            self.completed = true;
        }
    };
};

const GcProof = struct { intent: contract.Intent, inode: u128 };

fn loadGcFromDir(alloc: std.mem.Allocator, io: std.Io, directory: std.Io.Dir, name: []const u8) !?GcProof {
    var file = directory.openFile(io, name, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);
    if ((try file.stat(io)).kind != .file) return error.InvalidInitialFkRetirementIntent;
    var buffer: [4096]u8 = undefined;
    var reader = file.reader(io, &buffer);
    const raw = try reader.interface.allocRemaining(alloc, .limited(4096));
    defer alloc.free(raw);
    var parsed = try std.json.parseFromSlice(GcProof, alloc, raw, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    try parsed.value.intent.ticket.validate();
    return parsed.value;
}

fn loadFromDir(alloc: std.mem.Allocator, io: std.Io, directory: std.Io.Dir, name: []const u8) !?contract.Intent {
    var file = directory.openFile(io, name, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);
    if ((try file.stat(io)).kind != .file) return error.InvalidInitialFkRetirementIntent;
    var buffer: [contract.intent_encoded_len + 1]u8 = undefined;
    var reader = file.reader(io, &buffer);
    const raw = try reader.interface.allocRemaining(alloc, .limited(contract.intent_encoded_len + 1));
    defer alloc.free(raw);
    return try contract.Intent.decode(raw);
}

pub fn requireExact(ticket: Ticket, observed: Observation) !void {
    return requireExactPhase(ticket, observed, false);
}

fn requireExactPhase(ticket: Ticket, observed: Observation, allow_hidden: bool) !void {
    const replica = observed.replica orelse return error.InitialFkRetirementProofUnavailable;
    if (replica.group_id != ticket.replica.group_id or replica.replica_id != ticket.replica.replica_id or
        replica.node_id != ticket.replica.node_id or replica.root_generation != ticket.replica.root_generation)
        return error.InitialFkRetirementWorkChanged;
    const record = observed.publication orelse return error.InitialFkRetirementProofUnavailable;
    try record.validate();
    const phase_allowed = record.phase == .canceled or (allow_hidden and record.phase == .hidden) or
        (ticket.replica.retirement_authority == .published_obsolete and record.phase == .released);
    if (!phase_allowed or !std.mem.eql(u8, &record.plan_id, &ticket.replica.plan_id) or
        !std.mem.eql(u8, &record.plan_digest, &ticket.replica.plan_digest) or
        record.namespace.table_id != ticket.replica.child_table_id or
        record.namespace.shard_id != ticket.replica.group_id or record.namespace.range_id != ticket.replica.range_id)
        return error.InitialChildPublicationChanged;
}

/// Exact-generation startup fence, including prepared work whose exact
/// publication has already been cold-proven. ACKed tombstones remain durable.
pub fn blocksReplica(alloc: std.mem.Allocator, io: std.Io, root: []const u8, replica: ReplicaIdentity) !bool {
    if (replica.root_generation == 0) return false;
    for ([_][]const u8{ "bin", "done" }, [_][]const u8{ journal_name, completed_name }) |suffix, directory| {
        const path = try std.fmt.allocPrint(alloc, "{s}/{s}/group-{d}-replica-{d}-generation-{d}.{s}", .{
            root, directory, replica.group_id, replica.replica_id, replica.root_generation, suffix,
        });
        defer alloc.free(path);
        const raw = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(contract.intent_encoded_len + 1)) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer alloc.free(raw);
        const intent = try contract.Intent.decode(raw);
        if (intent.ticket.replica.group_id != replica.group_id or intent.ticket.replica.replica_id != replica.replica_id or
            intent.ticket.replica.node_id != replica.node_id or intent.ticket.replica.root_generation != replica.root_generation)
            return error.InvalidInitialFkRetirementIntent;
        return true;
    }
    return false;
}

/// Private provisioning knows the AICH plan before its replica descriptor.
/// A canceled plan cannot be resurrected while its exact catalog removal or
/// ACK is being retried, even if a stale private-provisioning request arrives.
pub fn blocksPlan(alloc: std.mem.Allocator, io: std.Io, root: []const u8, group_id: u64, plan_id: [16]u8, plan_digest: [32]u8) !bool {
    inline for (.{ journal_name, completed_name }) |directory| {
        if (try blocksPlanInDirectory(alloc, io, root, directory, group_id, plan_id, plan_digest)) return true;
    }
    return false;
}

fn blocksPlanInDirectory(alloc: std.mem.Allocator, io: std.Io, root: []const u8, directory: []const u8, group_id: u64, plan_id: [16]u8, plan_digest: [32]u8) !bool {
    const path = try std.fs.path.join(alloc, &.{ root, directory });
    defer alloc.free(path);
    var dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer dir.close(io);
    var prefix_buf: [64]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&prefix_buf, "group-{d}-", .{group_id});
    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != .file or !std.mem.startsWith(u8, entry.name, prefix) or
            (!std.mem.endsWith(u8, entry.name, ".bin") and !std.mem.endsWith(u8, entry.name, ".done"))) continue;
        const raw = try dir.readFileAlloc(io, entry.name, alloc, .limited(contract.intent_encoded_len + 1));
        defer alloc.free(raw);
        const intent = try contract.Intent.decode(raw);
        if (intent.ticket.replica.group_id == group_id and std.mem.eql(u8, &intent.ticket.replica.plan_id, &plan_id) and
            std.mem.eql(u8, &intent.ticket.replica.plan_digest, &plan_digest)) return true;
    }
    return false;
}

fn isDirectory(io: std.Io, path: []const u8) !bool {
    const stat = std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    if (stat.kind != .directory) return error.InitialFkRetirementPathChanged;
    return true;
}

fn ensureDirectory(io: std.Io, path: []const u8) !void {
    if (try isDirectory(io, path)) return;
    try fs.createDirPathPortable(io, path);
    if (!try isDirectory(io, path)) return error.InitialFkRetirementPathChanged;
}

const Paths = struct {
    journal_dir: []u8,
    done_dir: []u8,
    trash_dir: []u8,
    intent_path: []u8,
    done_path: []u8,
    group_path: []u8,
    trash_path: []u8,

    fn init(alloc: std.mem.Allocator, root: []const u8, ticket: Ticket) !Paths {
        const digest = std.fmt.bytesToHex(try ticket.digest(), .lower);
        const journal_dir = try std.fs.path.join(alloc, &.{ root, journal_name });
        errdefer alloc.free(journal_dir);
        const done_dir = try std.fs.path.join(alloc, &.{ root, completed_name });
        errdefer alloc.free(done_dir);
        const trash_dir = try std.fs.path.join(alloc, &.{ root, trash_name });
        errdefer alloc.free(trash_dir);
        const intent_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}-replica-{d}-generation-{d}.bin", .{
            journal_dir, ticket.replica.group_id, ticket.replica.replica_id, ticket.replica.root_generation,
        });
        errdefer alloc.free(intent_path);
        const done_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}-replica-{d}-generation-{d}.done", .{
            done_dir, ticket.replica.group_id, ticket.replica.replica_id, ticket.replica.root_generation,
        });
        errdefer alloc.free(done_path);
        const group_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ root, ticket.replica.group_id });
        errdefer alloc.free(group_path);
        return .{ .journal_dir = journal_dir, .done_dir = done_dir, .trash_dir = trash_dir, .intent_path = intent_path, .done_path = done_path, .group_path = group_path, .trash_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ trash_dir, digest }) };
    }
    pub fn deinit(self: *Paths, alloc: std.mem.Allocator) void {
        alloc.free(self.journal_dir);
        alloc.free(self.done_dir);
        alloc.free(self.trash_dir);
        alloc.free(self.intent_path);
        alloc.free(self.done_path);
        alloc.free(self.group_path);
        alloc.free(self.trash_path);
    }
};

const TestOwner = struct {
    worker: *Worker,
    ticket: Ticket,
    catalog_present: bool = true,
    root_generation: u64 = 27,
    held: bool = false,
    ack_count: usize = 0,
    last_reporter: u64 = 0,
    fail_at: ?FaultPoint = null,

    fn acquire(ptr: *anyopaque, _: Ticket) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expect(!self.held);
        self.held = true;
    }
    fn release(ptr: *anyopaque, _: Ticket) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        std.debug.assert(self.held);
        self.held = false;
    }
    fn observe(ptr: *anyopaque, _: Ticket, group_path: []const u8) !Observation {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expect(self.held);
        const path = try std.fs.path.join(self.worker.alloc, &.{ group_path, "aich.bin" });
        defer self.worker.alloc.free(path);
        const raw = try std.Io.Dir.cwd().readFileAlloc(self.worker.io, path, self.worker.alloc, .limited(4096));
        defer self.worker.alloc.free(raw);
        return .{ .replica = if (self.catalog_present) .{ .group_id = self.ticket.replica.group_id, .replica_id = self.ticket.replica.replica_id, .node_id = self.ticket.replica.node_id, .root_generation = self.root_generation } else null, .publication = try publication.Record.decode(raw) };
    }
    fn quiesce(ptr: *anyopaque, _: Ticket, action: Action) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expect(self.held);
        try action.run(action.ptr);
    }
    fn cancelHidden(ptr: *anyopaque, _: Ticket, _: []const u8, expected: publication.Record) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expect(self.held);
        try std.testing.expectEqual(publication.Phase.hidden, expected.phase);
        try testWritePublication(self.worker, .canceled);
    }
    fn remove(ptr: *anyopaque, ticket: Ticket) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expect(self.held);
        if (self.root_generation == ticket.replica.root_generation) self.catalog_present = false;
    }
    fn ack(ptr: *anyopaque, signed: auth.SignedReceipt) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expect(!self.held);
        try std.testing.expect(!self.catalog_present or self.root_generation != signed.receipt.ticket.replica.root_generation);
        try signed.verify(self.worker.root.public_key);
        self.ack_count += 1;
        self.last_reporter = signed.receipt.reporter_incarnation;
    }
    fn fault(ptr: *anyopaque, point: FaultPoint) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.fail_at == point) return error.InjectedRetirementCrash;
    }
};

fn testTicket() Ticket {
    return .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .cancel_revision = 29, .replica = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .child_table_id = 3,
        .group_id = 5,
        .range_id = 7,
        .node_id = 11,
        .store_id = 13,
        .store_incarnation = 17,
        .store_root_incarnation = 19,
        .replica_id = 23,
        .root_generation = 27,
        .canceled = true,
    } };
}

fn testWritePublication(worker: *Worker, phase: publication.Phase) !void {
    const ticket = testTicket();
    const record: publication.Record = .{
        .phase = phase,
        .plan_id = ticket.replica.plan_id,
        .plan_digest = ticket.replica.plan_digest,
        .namespace = .{ .table_id = 3, .shard_id = 5, .range_id = 7 },
        .schema_version = 1,
        .row_count = 0,
        .schema_digest = @splat(3),
        .public_schema_json_digest = @splat(4),
        .catalog_digest = @splat(5),
        .provision_term = 1,
        .provision_index = 1,
        .phase_term = if (phase == .hidden) 0 else 1,
        .phase_index = if (phase == .hidden) 0 else 2,
    };
    var paths = try Paths.init(worker.alloc, worker.replica_root_dir, ticket);
    defer paths.deinit(worker.alloc);
    try fs.createDirPathPortable(worker.io, paths.group_path);
    const file_path = try std.fs.path.join(worker.alloc, &.{ paths.group_path, "aich.bin" });
    defer worker.alloc.free(file_path);
    var file = try fs.createFilePortable(worker.io, file_path, .{ .truncate = true });
    defer file.close(worker.io);
    var buffer: [512]u8 = undefined;
    var writer = file.writer(worker.io, &buffer);
    try writer.interface.writeAll(&try record.encode());
    try writer.end();
    try file.sync(worker.io);
}

fn testWorker(alloc: std.mem.Allocator, io: std.Io, root: []const u8, owner: *TestOwner) !Worker {
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    return .{
        .alloc = alloc,
        .io = io,
        .replica_root_dir = root,
        .node_id = 11,
        .store_id = 13,
        .reporter_incarnation = 31,
        .metadata_incarnation = testTicket().metadata_incarnation,
        .root = .{ .root_incarnation = 19, .seed = @splat(7), .public_key = pair.public_key.toBytes() },
        .callbacks = .{ .ptr = owner, .acquire = TestOwner.acquire, .release = TestOwner.release, .observe = TestOwner.observe, .quiesce = TestOwner.quiesce, .remove_catalog_exact = TestOwner.remove, .acknowledge = TestOwner.ack },
        .fault = .{ .ptr = owner, .hit = TestOwner.fault },
    };
}

test "FK retirement worker recovers every durable unlink and ACK crash boundary" {
    const alloc = std.testing.allocator;
    var io_impl = platform.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    inline for (std.meta.tags(FaultPoint)) |point| {
        var tmp = platform.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer alloc.free(root);
        var worker: Worker = undefined;
        var owner: TestOwner = .{ .worker = &worker, .ticket = testTicket(), .fail_at = point };
        worker = try testWorker(alloc, io_impl.io(), root, &owner);
        try testWritePublication(&worker, .canceled);
        try std.testing.expectError(error.InjectedRetirementCrash, worker.process(testTicket()));
        try std.testing.expect(!owner.held);
        const identity: ReplicaIdentity = .{ .group_id = 5, .replica_id = 23, .node_id = 11, .root_generation = 27 };
        try std.testing.expect(try blocksReplica(alloc, worker.io, root, identity));
        try std.testing.expect(try blocksPlan(alloc, worker.io, root, 5, testTicket().replica.plan_id, testTicket().replica.plan_digest));
        try std.testing.expectEqual(@as(usize, if (point == .acknowledged) 1 else 0), owner.ack_count);
        // Restart retains only the durable journal/trash and physical catalog;
        // the reporter's process incarnation changes, root signing key does not.
        owner.fail_at = null;
        worker.reporter_incarnation += 1;
        try std.testing.expectEqual(@as(usize, 1), try worker.recover(8));
        try std.testing.expectEqual(@as(u64, 32), owner.last_reporter);
        try std.testing.expect(!owner.catalog_present);
        var paths = try Paths.init(alloc, root, testTicket());
        defer paths.deinit(alloc);
        try std.testing.expect(!try isDirectory(worker.io, paths.group_path));
        try std.testing.expect(try isDirectory(worker.io, paths.trash_path));
        try std.testing.expect((try worker.load(paths.intent_path)) == null);
        try std.testing.expect(try blocksReplica(alloc, worker.io, root, identity));
        var replacement = identity;
        replacement.root_generation += 1;
        try std.testing.expect(!try blocksReplica(alloc, worker.io, root, replacement));
        try std.testing.expectEqual(@as(usize, 0), try worker.recover(8));
    }
}

test "FK retirement worker refuses replaced roots hidden publication and missing proof" {
    const alloc = std.testing.allocator;
    var io_impl = platform.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    var worker: Worker = undefined;
    var owner: TestOwner = .{ .worker = &worker, .ticket = testTicket() };
    worker = try testWorker(alloc, io_impl.io(), root, &owner);
    try testWritePublication(&worker, .hidden);
    try std.testing.expectError(error.InitialChildPublicationChanged, worker.process(testTicket()));
    try testWritePublication(&worker, .canceled);
    owner.root_generation += 1;
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, worker.process(testTicket()));
    owner.root_generation -= 1;
    var wrong = testTicket();
    wrong.replica.store_root_incarnation += 1;
    try std.testing.expectError(error.InitialFkRetirementRootChanged, worker.process(wrong));
    owner.catalog_present = false;
    try std.testing.expectError(error.InitialFkRetirementProofUnavailable, worker.process(testTicket()));
    var paths = try Paths.init(alloc, root, testTicket());
    defer paths.deinit(alloc);
    try std.testing.expect(try isDirectory(worker.io, paths.group_path));
    try std.testing.expect((try worker.load(paths.intent_path)) == null);
    try std.testing.expectEqual(@as(usize, 0), owner.ack_count);
}

test "FK retirement worker terminal ticket cancels offline hidden owner before unlink" {
    const alloc = std.testing.allocator;
    var io_impl = platform.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    var worker: Worker = undefined;
    var owner: TestOwner = .{ .worker = &worker, .ticket = testTicket() };
    worker = try testWorker(alloc, io_impl.io(), root, &owner);
    worker.callbacks.cancel_hidden = TestOwner.cancelHidden;
    try testWritePublication(&worker, .hidden);
    owner.fail_at = .canceled;
    try std.testing.expectError(error.InjectedRetirementCrash, worker.process(testTicket()));
    try std.testing.expectEqual(@as(usize, 0), owner.ack_count);
    owner.fail_at = null;
    try std.testing.expectEqual(@as(usize, 1), try worker.recover(1));
    try std.testing.expectEqual(@as(usize, 1), owner.ack_count);
    var paths = try Paths.init(alloc, root, testTicket());
    defer paths.deinit(alloc);
    const raw_path = try std.fs.path.join(alloc, &.{ paths.trash_path, "aich.bin" });
    defer alloc.free(raw_path);
    const raw = try std.Io.Dir.cwd().readFileAlloc(worker.io, raw_path, alloc, .limited(4096));
    defer alloc.free(raw);
    try std.testing.expectEqual(publication.Phase.canceled, (try publication.Record.decode(raw)).phase);
}

test "FK retirement worker published obsolete released root survives crash without cancellation or replacement deletion" {
    const alloc = std.testing.allocator;
    var io_impl = platform.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    var worker: Worker = undefined;
    var owner: TestOwner = .{ .worker = &worker, .ticket = testTicket() };
    worker = try testWorker(alloc, io_impl.io(), root, &owner);
    worker.callbacks.cancel_hidden = TestOwner.cancelHidden; // Would reject released.
    try testWritePublication(&worker, .released);
    try std.testing.expectError(error.InitialChildPublicationChanged, worker.process(owner.ticket));
    owner.ticket.replica.retirement_authority = .published_obsolete;
    owner.fail_at = .unlinked;
    try std.testing.expectError(error.InjectedRetirementCrash, worker.process(owner.ticket));
    var paths = try Paths.init(alloc, root, owner.ticket);
    defer paths.deinit(alloc);
    const raw_path = try std.fs.path.join(alloc, &.{ paths.trash_path, "aich.bin" });
    defer alloc.free(raw_path);
    const raw = try std.Io.Dir.cwd().readFileAlloc(worker.io, raw_path, alloc, .limited(4096));
    defer alloc.free(raw);
    try std.testing.expectEqual(publication.Phase.released, (try publication.Record.decode(raw)).phase);
    // A replacement catalog installed after durable unlink is not the old root.
    owner.root_generation += 1;
    owner.fail_at = null;
    try testWritePublication(&worker, .released);
    try std.testing.expectEqual(@as(usize, 1), try worker.recover(8));
    try std.testing.expect(owner.catalog_present);
    try std.testing.expectEqual(@as(usize, 1), owner.ack_count);
    _ = try worker.collectTrash(.{});
    try std.testing.expect(try isDirectory(worker.io, paths.group_path));
    try std.testing.expect(!try isDirectory(worker.io, paths.trash_path));
}

test "FK retirement worker trash GC is bounded restartable no-follow and replacement fenced" {
    const alloc = std.testing.allocator;
    var io_impl = platform.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root_path);
    var worker: Worker = undefined;
    var owner: TestOwner = .{ .worker = &worker, .ticket = testTicket() };
    worker = try testWorker(alloc, io_impl.io(), root_path, &owner);
    try testWritePublication(&worker, .canceled);
    try worker.process(testTicket());
    var paths = try Paths.init(alloc, root_path, testTicket());
    defer paths.deinit(alloc);
    var root = try std.Io.Dir.cwd().openDir(worker.io, root_path, .{});
    defer root.close(worker.io);
    try root.createDir(worker.io, "outside", .default_dir);
    var outside = try root.openDir(worker.io, "outside", .{});
    defer outside.close(worker.io);
    (try outside.createFile(worker.io, "sentinel", .{})).close(worker.io);
    var trash = try root.openDir(worker.io, trash_name, .{});
    defer trash.close(worker.io);
    const name = std.fs.path.basename(paths.trash_path);
    var retired = try trash.openDir(worker.io, name, .{});
    defer retired.close(worker.io);
    try retired.createDir(worker.io, "nested", .default_dir);
    var nested = try retired.openDir(worker.io, "nested", .{});
    defer nested.close(worker.io);
    for (0..8) |i| {
        var buf: [32]u8 = undefined;
        (try nested.createFile(worker.io, try std.fmt.bufPrint(&buf, "file-{d}", .{i}), .{})).close(worker.io);
    }
    try retired.symLink(worker.io, "../../outside", "malicious-link", .{ .is_directory = true });
    try trash.symLink(worker.io, "../outside", &(@as([64]u8, @splat('0'))), .{ .is_directory = true });
    // A same-name replacement is not the inode bound at retirement. Neither
    // GC nor an old metadata page may authorize it using the old tombstone.
    try std.Io.Dir.rename(trash, name, root, "saved-retired-root", worker.io);
    try trash.createDir(worker.io, name, .default_dir);
    var replacement = try trash.openDir(worker.io, name, .{});
    (try replacement.createFile(worker.io, "replacement", .{})).close(worker.io);
    replacement.close(worker.io);
    try std.testing.expectEqual(@as(usize, 0), (try worker.collectTrash(.{})).deleted);
    try std.testing.expectError(error.InitialFkRetirementPathChanged, worker.process(testTicket()));
    replacement = try trash.openDir(worker.io, name, .{});
    try replacement.deleteFile(worker.io, "replacement");
    replacement.close(worker.io);
    try trash.deleteDir(worker.io, name);
    try std.Io.Dir.rename(root, "saved-retired-root", trash, name, worker.io);
    try std.testing.expectEqual(@as(usize, 0), (try worker.collectTrash(.{ .max_ns = 0 })).deleted);
    for (0..32) |_| {
        // Reconstructing the worker simulates restart between every slice.
        worker = try testWorker(alloc, io_impl.io(), root_path, &owner);
        const result = try worker.collectTrash(.{ .max_entries = 64, .max_deletes = 1, .max_ns = std.time.ns_per_s });
        try std.testing.expect(result.deleted <= 1);
        if (!try isDirectory(worker.io, paths.trash_path)) break;
    } else return error.GarbageCollectionDidNotConverge;
    _ = try outside.statFile(worker.io, "sentinel", .{ .follow_symlinks = false });
    try std.testing.expect((try worker.load(paths.done_path)) != null);
    try std.testing.expect(try blocksReplica(alloc, worker.io, root_path, .{ .group_id = 5, .replica_id = 23, .node_id = 11, .root_generation = 27 }));
}

test "FK retirement worker recovery cursor skips bounded poison entries without starvation" {
    const alloc = std.testing.allocator;
    var io_impl = platform.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var tmp = platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root_path);
    var worker: Worker = undefined;
    var owner: TestOwner = .{ .worker = &worker, .ticket = testTicket(), .fail_at = .prepared };
    worker = try testWorker(alloc, io_impl.io(), root_path, &owner);
    try testWritePublication(&worker, .canceled);
    var paths = try Paths.init(alloc, root_path, testTicket());
    defer paths.deinit(alloc);
    try fs.createDirPathPortable(worker.io, paths.journal_dir);
    var dir = try std.Io.Dir.cwd().openDir(worker.io, paths.journal_dir, .{});
    defer dir.close(worker.io);
    for (0..20) |i| {
        var buffer: [32]u8 = undefined;
        (try dir.createFile(worker.io, try std.fmt.bufPrint(&buffer, "poison-{d}.bin", .{i}), .{})).close(worker.io);
    }
    try std.testing.expectError(error.InjectedRetirementCrash, worker.process(testTicket()));
    owner.fail_at = null;
    var cursor: RecoveryCursor = .{};
    defer cursor.deinit();
    worker.recovery_cursor = &cursor;
    const Stop = struct {
        fn check(_: *anyopaque) bool {
            return false;
        }
    };
    worker.continue_work = .{ .ptr = &owner, .check = Stop.check };
    try std.testing.expectEqual(@as(usize, 0), try worker.recover(1));
    try std.testing.expectEqual(@as(usize, 0), owner.ack_count);
    worker.continue_work = null;
    for (0..24) |_| {
        const count = try worker.recover(1);
        try std.testing.expect(count <= 1);
        if (owner.ack_count == 1) break;
    } else return error.RecoveryStarved;
}
