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

//! A sealed private working set publishes directory, writer and obsolete roots
//! together. It does not publish Backend state or claim durable completion.
//! Every captured source, partial result and retired generation has one owner.
const std = @import("std");
const resource = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Directory = @import("run_directory.zig").Directory;
const Store = @import("run_store.zig").Store;
const Ledger = @import("obsolete_ledger.zig").Ledger;
const Run = @import("repository.zig").Run;
const Record = @import("registered_writer.zig").Record;
const Account = @import("memory_account.zig").Account;
const DirEdit = @import("registered_directory_root.zig").Candidate;
const WriterEdit = @import("registered_writer_root.zig").Candidate;
const ObsoleteEdit = @import("registered_obsolete.zig").Candidate;

/// Exclusive value ownership. Copying this header does not acquire a lease.
/// Callers fund the header and serialize access against publish/retire.
/// Nonempty sets are created only by successful Candidate.publishWithAdmission.
pub const Roots = struct {
    directory: ?*Directory = null,
    writer: ?*Store = null,
    obsolete: Ledger = .{},
    seal: ?*Candidate = null,

    pub const empty: Roots = .{};
    pub fn retire(self: *Roots) void {
        const owner = self.seal orelse {
            std.debug.assert(self.directory == null and self.writer == null and self.obsolete.tree.account == null);
            return;
        };
        std.debug.assert(!owner.returned_ready.load(.acquire));
        owner.returned = self.*;
        owner.returned.seal = null;
        self.* = .{};
        owner.returned_ready.store(true, .release);
        owner.requestRetirement();
        owner.release(); // No owner access follows this final fixed drop.
    }
};
const Stamp = struct {
    pointers: [11]?*const anyopaque = @splat(null),
    bytes: u64 = 0,
    memory: usize = 0,
    fn of(roots: *const Roots) Stamp {
        var out: Stamp = .{};
        if (roots.directory) |directory| {
            inline for (.{ "tree", "ids", "bounds", "ends", "levels", "generations" }, 0..) |field, i| out.pointers[i] = @ptrCast(@field(directory, field).root);
            out.pointers[6] = directory.registered_owner;
            out.bytes = directory.total_run_bytes;
            out.memory = directory.memory_run_count;
        }
        if (roots.writer) |writer| {
            out.pointers[7] = writer.tree.root;
            out.pointers[8] = writer.registered_owner;
        }
        out.pointers[9] = roots.obsolete.tree.root;
        out.pointers[10] = roots.obsolete.registered_owner;
        return out;
    }
    fn matches(self: Stamp, roots: *const Roots) bool {
        return std.meta.eql(self, of(roots));
    }
};

/// A Backend may borrow these headers only while its publication mutex and
/// their exclusive owner serialize access. This check neither acquires nor
/// transfers ownership; it verifies every root and its accounting guards.
pub fn matchesBackend(roots: *const Roots, backend: anytype) bool {
    if (backend.run_directory != roots.directory) {
        if (roots.seal != null or roots.directory != null) return false;
        const legacy = backend.run_directory orelse return false;
        if (!exclusiveEmptyLegacyDirectory(legacy, backend.allocator)) return false;
    }
    if (roots.writer) |writer| {
        if (backend.runs.tree.root != writer.tree.root or backend.runs.tree.account != writer.tree.account or backend.runs.tree.spare.capacity != writer.tree.spare.capacity or backend.runs.registered_owner != writer.registered_owner or backend.runs.registered_account != writer.registered_account) return false;
    } else if (backend.runs.tree.root != null or backend.runs.tree.account != null or backend.runs.tree.spare.capacity != 0 or backend.runs.registered_owner != null or backend.runs.registered_account != null) return false;
    return backend.obsolete_paths.tree.root == roots.obsolete.tree.root and backend.obsolete_paths.tree.account == roots.obsolete.tree.account and backend.obsolete_paths.registered_owner == roots.obsolete.registered_owner and backend.obsolete_paths.tree.spare.capacity == roots.obsolete.tree.spare.capacity and backend.obsolete_paths.spare.capacity == roots.obsolete.spare.capacity;
}

/// Empty legacy caches contain no semantic run data. Only exclusively owned,
/// unfunded accounts may transfer: a foreign mutable fork cannot outlive the
/// funded header handoff or allocate through an escaped legacy account.
fn exclusiveEmptyLegacyDirectory(directory: *const Directory, a: std.mem.Allocator) bool {
    if (directory.registered_owner != null or directory.registered_account != null or directory.total_run_bytes != 0 or directory.memory_run_count != 0) return false;
    const fields = .{ "tree", "ids", "bounds", "ends", "levels", "generations" };
    inline for (fields) |field| {
        const tree = @field(directory, field);
        if (tree.root != null or tree.spare.capacity != 0) return false;
        if (tree.account) |account| {
            if (account.independently_funded or account.backing.ptr != a.ptr or account.backing.vtable != a.vtable or account.bytes.load(.acquire) != @sizeOf(Account)) return false;
            var internal_refs: usize = 0;
            inline for (fields) |other| internal_refs += @intFromBool(@field(directory, other).account == account);
            if (account.refs.load(.acquire) != internal_refs) return false;
        }
    }
    return true;
}

pub const Candidate = struct {
    const Input = struct {
        held: Directory.RegisteredSource,
        run: Run,
        removing: bool,
        deadline: u64,
    };
    const State = struct {
        directory: Directory = .{},
        writer: Store = .{},
        obsolete: Ledger = .{},
        source_seal: ?*Candidate = null,
        held: ?Directory.RegisteredSource = null,
        run: ?Run = null,
        id_search: ?Directory.IdSearch = null,
        checked_id: bool = false,
        directory_child: ?*DirEdit = null,
        writer_child: ?*WriterEdit = null,
        obsolete_child: ?*ObsoleteEdit = null,
        outputs: Roots = .{},
        displaced: Roots = .{},
        next_input: ?Input = null,
        directory_applied: bool = false,
        writer_applied: bool = false,
        obsolete_applied: bool = false,
        dir_cleanup: ?Directory.Reclaimer = null,
        writer_cleanup: ?Store.Reclaimer = null,
        ledger_cleanup: ?Ledger.Reclaimer = null,
        source_phase: usize = 0,
    };
    allocator: std.mem.Allocator,
    manager: *resource.ResourceManager,
    host: resource.AllocatorHost,
    registration: resource.PayloadRetirement = .{},
    credit: ?resource.Reservation = null,
    refs: std.atomic.Value(usize) = .init(1),
    state: ?*State = null,
    state_offset: usize = 0,
    state_retiring: bool = false,
    source_stamp: Stamp = .{},
    expected: Stamp = .{},
    returned: Roots = .{},
    returned_ready: std.atomic.Value(bool) = .init(false),
    source_captured: bool = false,
    source_generation: ?u64 = null,
    input_revision: u64 = 0,
    removing: bool = false,
    deadline: u64 = 0,
    ready: bool = false,
    published: bool = false,
    closing: bool = false,
    complete: bool = false,

    fn validateRoots(roots: *const Roots, a: std.mem.Allocator, original: admission.Port) !void {
        try original.checkpoint();
        return validateRootsFixed(roots, a);
    }
    fn validateRootsFixed(roots: *const Roots, a: std.mem.Allocator) !void {
        const seal = roots.seal orelse {
            if (roots.directory != null or roots.writer != null or roots.obsolete.tree.account != null or roots.obsolete.registered_owner != null or roots.obsolete.count() != 0 or roots.obsolete.spare.capacity != 0 or roots.obsolete.tree.spare.capacity != 0) return error.InvalidArgument;
            return;
        };
        if (seal.registration.step == null or seal.registration.step.?.advance != advance or seal.registration.context != @as(*anyopaque, @ptrCast(seal)) or seal.registration.credit == null or seal.registration.credit.?.manager != seal.manager or !seal.published or seal.complete or seal.returned_ready.load(.acquire) or seal.refs.load(.acquire) < 2 or !seal.expected.matches(roots)) return error.InvalidArgument;
        const directory = roots.directory orelse return error.InvalidArgument;
        const writer = roots.writer orelse return error.InvalidArgument;
        _ = try DirEdit.validateSourceFixed(directory, a);
        try WriterEdit.validateSourceFixed(writer, a);
        try ObsoleteEdit.validateSourceFixed(&roots.obsolete, a);
    }
    fn validatePair(held: *const Directory.RegisteredSource, run: *const Run, original: admission.Port) !void {
        const record = try Record.validate(run, original);
        if (record.source.?.lease.?.entry.payload != held.lease.?.entry.payload or record.run.level != held.run().level or record.run.gc_requested != held.run().gc_requested) return error.InvalidArgument;
    }
    pub fn startPut(a: std.mem.Allocator, manager: *resource.ResourceManager, roots: *const Roots, lease: *const Directory.EntryLease, run: *const Run, scope: admission.Port) !*Candidate {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try validateRoots(roots, a, original);
        var held = try lease.retainRegisteredSourceWithAdmission(original);
        defer held.releaseWithAdmission(null) catch unreachable;
        try validatePair(&held, run, original);
        const self = try prepare(a, manager, original);
        errdefer self.requestRetirement();
        try self.captureRootsWithAdmission(roots, original);
        self.state.?.held = try lease.retainRegisteredSourceWithAdmission(original);
        try original.checkpoint();
        self.state.?.run = run.retainOwned();
        return self;
    }
    pub fn startRemove(a: std.mem.Allocator, manager: *resource.ResourceManager, roots: *const Roots, handle: Directory.Handle, run: *const Run, deadline: u64, scope: admission.Port) !*Candidate {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try validateRoots(roots, a, original);
        if (roots.directory == null or roots.writer == null) return error.CompactionPlanningStale;
        var held = try handle.retainRegisteredSourceWithAdmission(original);
        defer held.releaseWithAdmission(null) catch unreachable;
        try validatePair(&held, run, original);
        const self = try prepare(a, manager, original);
        errdefer self.requestRetirement();
        try self.captureRootsWithAdmission(roots, original);
        self.removing = true;
        self.deadline = deadline;
        self.state.?.held = try handle.retainRegisteredSourceWithAdmission(original);
        try original.checkpoint();
        self.state.?.run = run.retainOwned();
        return self;
    }
    /// Prepay/register the frame and empty snapshot state before entering a
    /// publication mutex. Capture and input selection are separate fixed tails.
    pub fn prepare(a: std.mem.Allocator, manager: *resource.ResourceManager, original: admission.Port) !*Candidate {
        var host: ?resource.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, @sizeOf(Candidate) + @sizeOf(State) + @sizeOf(Directory) + 6 * @sizeOf(Account), original);
        defer construction.retire();
        const self = try a.create(Candidate);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.? };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        try original.checkpoint();
        self.state = try a.create(State);
        self.state.?.* = .{};
        return self;
    }
    /// Caller holds the publication mutex and owns the source tuple. Only
    /// lifetime checks and fixed root/account/seal retains occur here.
    pub fn captureRootsWithAdmission(self: *Candidate, roots: *const Roots, original: admission.Port) !void {
        try original.checkLifetime();
        if (self.closing or self.published or self.source_captured or self.state == null) return error.InvalidArgument;
        try validateRootsFixed(roots, self.allocator);
        self.source_stamp = .of(roots);
        if (roots.seal) |seal| {
            _ = seal.refs.fetchAdd(1, .monotonic);
            self.state.?.source_seal = seal;
        }
        if (roots.directory) |directory| self.state.?.directory = directory.forkContents();
        if (roots.writer) |writer| self.state.?.writer = writer.fork();
        self.state.?.obsolete = roots.obsolete.fork();
        self.source_captured = true;
    }
    /// Fixed validation for a serialized caller. No Backend borrow is stored.
    pub fn validateCapturedSourceWithAdmission(self: *Candidate, roots: *const Roots, original: admission.Port) !void {
        try original.checkLifetime();
        if (self.closing or self.published or !self.source_captured or self.state == null) return error.InvalidArgument;
        if (roots.seal != self.state.?.source_seal or !self.source_stamp.matches(roots)) return error.CompactionPlanningStale;
    }
    /// Borrow only until the next append/resume/retirement. The private tuple
    /// has no publication lease and cannot be independently retired or copied.
    pub fn preparedRootsWithAdmission(self: *Candidate, original: admission.Port) !*const Roots {
        try original.checkpoint();
        if (self.closing or self.published or !self.ready or self.state == null) return error.InvalidArgument;
        return &self.state.?.outputs;
    }
    /// A synchronous borrow of this exact completed edit sequence. The owner
    /// remains alive until validation is abandoned or installation finishes.
    /// Revisions prevent an append/retire/rebuild ABA of private root pointers.
    pub const PreparedReceipt = struct { owner: *Candidate, revision: u64 };
    pub fn preparedReceiptWithAdmission(self: *Candidate, original: admission.Port) !PreparedReceipt {
        _ = try self.preparedRootsWithAdmission(original);
        return .{ .owner = self, .revision = self.input_revision };
    }
    pub fn validatePreparedReceiptFixedWithAdmission(self: *Candidate, receipt: PreparedReceipt, original: admission.Port) !void {
        try original.checkLifetime();
        if (receipt.owner != self or receipt.revision != self.input_revision or self.closing or self.published or !self.ready or self.state == null) return error.CompactionPlanningStale;
    }
    pub fn appendPutWithAdmission(self: *Candidate, lease: *const Directory.EntryLease, run: *const Run, original: admission.Port) !void {
        var held = try lease.retainRegisteredSourceWithAdmission(original);
        errdefer held.releaseWithAdmission(null) catch unreachable;
        try validatePair(&held, run, original);
        try self.appendWithAdmission(held, run, false, 0, original);
    }
    pub fn appendRemoveWithAdmission(self: *Candidate, handle: Directory.Handle, run: *const Run, deadline: u64, original: admission.Port) !void {
        var held = try handle.retainRegisteredSourceWithAdmission(original);
        errdefer held.releaseWithAdmission(null) catch unreachable;
        try validatePair(&held, run, original);
        try self.appendWithAdmission(held, run, true, deadline, original);
    }
    fn appendWithAdmission(self: *Candidate, held: Directory.RegisteredSource, run: *const Run, removing: bool, deadline: u64, original: admission.Port) !void {
        try original.checkpoint();
        if (self.closing or self.published or !self.source_captured or self.state == null or self.state.?.next_input != null) return error.InvalidArgument;
        if (self.input_revision == std.math.maxInt(u64)) return error.ResourceBudgetExceeded;
        if (self.state.?.run == null) {
            self.state.?.held = held;
            self.state.?.run = run.retainOwned();
            self.removing = removing;
            self.deadline = deadline;
            self.input_revision += 1;
            return;
        }
        if (!self.ready) return error.InvalidArgument;
        // Capture the complete next input before changing edit state. Retained
        // sources and partial rotation remain registered across every refusal.
        self.state.?.next_input = .{ .held = held, .run = run.retainOwned(), .removing = removing, .deadline = deadline };
        self.input_revision += 1;
        self.ready = false;
    }
    fn drainDisplaced(self: *Candidate, s: *State, original: admission.Port) !void {
        while (true) {
            try original.checkpoint();
            if (try self.rootsStep(&s.displaced, s, original)) return;
        }
    }
    pub fn resumeWithAdmission(self: *Candidate, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.state == null or self.published or !self.source_captured or self.state.?.run == null) return error.InvalidArgument;
        if (self.ready) return;
        const s = self.state.?;
        try self.drainDisplaced(s, original);
        if (s.next_input) |next| {
            if (s.held) |*held| try held.releaseWithAdmission(original);
            s.held = null;
            if (s.run) |*run| run.deinit(self.allocator);
            s.held = next.held;
            s.run = next.run;
            self.removing = next.removing;
            self.deadline = next.deadline;
            s.next_input = null;
            s.checked_id = false;
            s.id_search = null;
            s.directory_applied = false;
            s.writer_applied = false;
            s.obsolete_applied = false;
        }
        if (!self.removing and !s.checked_id) {
            if (s.id_search == null) s.id_search = (s.outputs.directory orelse &s.directory).locateId(&s.run.?);
            try s.id_search.?.resumeWithAdmission(original);
            if (s.id_search.?.matched != null) return error.DuplicateRun;
            s.id_search = null;
            s.checked_id = true;
        }
        if (!s.directory_applied) {
            if (s.directory_child == null) {
                if (self.removing) {
                    const lease = &s.held.?.lease.?;
                    s.directory_child = try DirEdit.startRemove(self.allocator, self.manager, s.outputs.directory orelse &s.directory, .{ .run = lease.entry.run, .revision = lease.entry.payload.? }, original);
                } else s.directory_child = try DirEdit.startPut(self.allocator, self.manager, s.outputs.directory orelse &s.directory, &s.held.?.lease.?, original);
            }
            try s.directory_child.?.resumeWithAdmission(original);
            const result = try s.directory_child.?.takeWithAdmission(original);
            s.displaced.directory = s.outputs.directory;
            s.outputs.directory = result;
            s.directory_applied = true;
            s.directory_child.?.requestRetirement();
            s.directory_child = null;
            try self.drainDisplaced(s, original);
        }
        if (!s.writer_applied) {
            if (s.writer_child == null) {
                if (self.removing) s.writer_child = try WriterEdit.startRemove(self.allocator, self.manager, s.outputs.writer orelse &s.writer, &s.run.?, original) else s.writer_child = try WriterEdit.start(self.allocator, self.manager, s.outputs.writer orelse &s.writer, &s.run.?, false, original);
            }
            try s.writer_child.?.resumeWithAdmission(original);
            const result = try s.writer_child.?.takeWithAdmission(original);
            s.displaced.writer = s.outputs.writer;
            s.outputs.writer = result;
            s.writer_applied = true;
            s.writer_child.?.requestRetirement();
            s.writer_child = null;
            try self.drainDisplaced(s, original);
        }
        if (!s.obsolete_applied) {
            const current = if (s.outputs.obsolete.tree.account != null) &s.outputs.obsolete else &s.obsolete;
            if (self.removing and s.run.?.path != null) {
                if (s.obsolete_child == null) s.obsolete_child = try ObsoleteEdit.startPut(self.allocator, self.manager, current, &s.run.?, self.deadline, original);
                try s.obsolete_child.?.resumeWithAdmission(original);
                const result = try s.obsolete_child.?.takeWithAdmission(original);
                s.displaced.obsolete = s.outputs.obsolete;
                s.outputs.obsolete = result;
                s.obsolete_child.?.requestRetirement();
                s.obsolete_child = null;
            } else {
                const result = current.fork();
                s.displaced.obsolete = s.outputs.obsolete;
                s.outputs.obsolete = result;
            }
            s.obsolete_applied = true;
            try self.drainDisplaced(s, original);
        }
        self.ready = true;
    }
    /// Publishes a caller-owned private working set only. Caller serializes
    /// access; the backend's durable publication and source check are separate.
    pub fn publishWithAdmission(self: *Candidate, live: *Roots, original: admission.Port) !void {
        try original.checkpoint();
        return self.publishPrepared(live);
    }
    /// Fixed publication tail for an already serialized Backend/owner. All
    /// refusals precede the swap; no cooperative checkpoint runs under its lock.
    pub fn publishFixedWithAdmission(self: *Candidate, live: *Roots, original: admission.Port) !void {
        try original.checkLifetime();
        return self.publishPrepared(live);
    }
    fn publishPrepared(self: *Candidate, live: *Roots) !void {
        if (self.closing or self.published or !self.ready or self.state == null) return error.InvalidArgument;
        if (live.seal != self.state.?.source_seal or !self.source_stamp.matches(live)) return error.CompactionPlanningStale;
        _ = self.refs.fetchAdd(1, .monotonic); // The new working-set lease.
        self.state.?.outputs.seal = self;
        std.mem.swap(Roots, live, &self.state.?.outputs);
        self.expected = .of(live);
        self.published = true;
    }
    /// Called only after a successful serialized bootstrap swap. The frame's
    /// original grant already covers this header and up to six empty accounts.
    /// No allocation, refusal or Backend borrow accompanies ownership transfer.
    pub fn adoptEmptyLegacyDirectory(self: *Candidate, directory: *Directory) void {
        std.debug.assert(self.published and !self.closing and self.state != null and self.state.?.displaced.directory == null);
        std.debug.assert(exclusiveEmptyLegacyDirectory(directory, self.allocator));
        self.state.?.displaced.directory = directory;
    }
    pub fn requestRetirement(self: *Candidate) void {
        if (self.closing) return;
        self.closing = true;
        self.registration.publish();
    }
    fn release(self: *Candidate) void {
        const previous = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 1);
    }
    fn rootsStep(self: *Candidate, roots: *Roots, s: *State, original: ?admission.Port) !bool {
        // A published tuple returns to its own final anchor. Its source
        // snapshots may still be held by this or another candidate.
        if (roots.seal != null) {
            roots.retire();
            return true;
        }
        if (roots.directory) |directory| {
            if (s.dir_cleanup == null) s.dir_cleanup = .init(directory);
            var steps: usize = 1;
            if (!s.dir_cleanup.?.step(self.allocator, &steps)) return false;
            if (original) |port| try port.checkpoint();
            s.dir_cleanup.?.finish(self.allocator);
            s.dir_cleanup = null;
            roots.directory = null;
            return false;
        }
        if (roots.writer) |writer| {
            if (s.writer_cleanup == null) s.writer_cleanup = .init(writer);
            var steps: usize = 1;
            if (!s.writer_cleanup.?.step(self.allocator, &steps)) return false;
            if (original) |port| try port.checkpoint();
            s.writer_cleanup.?.finish(self.allocator);
            s.writer_cleanup = null;
            roots.writer = null;
            return false;
        }
        if (roots.obsolete.tree.account != null) {
            if (s.ledger_cleanup == null) s.ledger_cleanup = .init(roots.obsolete);
            var steps: usize = 1;
            if (!s.ledger_cleanup.?.step(self.allocator, &steps)) return false;
            s.ledger_cleanup = null;
            roots.obsolete = .{};
            return false;
        }
        const seal = roots.seal;
        roots.seal = null;
        if (seal) |held| held.release();
        return true;
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Candidate = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.state) |s| {
            if (!self.state_retiring) {
                if (s.directory_child) |child| child.requestRetirement();
                s.directory_child = null;
                if (s.writer_child) |child| child.requestRetirement();
                s.writer_child = null;
                if (s.obsolete_child) |child| child.requestRetirement();
                s.obsolete_child = null;
                // Construction may have stopped in this exact displaced-root
                // reclaimer. Finish it before reusing the cleanup cursors.
                if (!try self.rootsStep(&s.displaced, s, original)) return false;
                if (!try self.rootsStep(&s.outputs, s, original)) return false;
                if (s.next_input) |*next| {
                    try next.held.releaseWithAdmission(original);
                    next.run.deinit(self.allocator);
                    s.next_input = null;
                }
                if (s.held) |*held| try held.releaseWithAdmission(original);
                s.held = null;
                if (s.run) |*run| run.deinit(self.allocator);
                s.run = null;
                if (s.source_phase == 0) {
                    if (s.dir_cleanup == null) s.dir_cleanup = .init(&s.directory);
                    var steps: usize = 1;
                    if (!s.dir_cleanup.?.step(self.allocator, &steps)) return false;
                    if (original) |port| try port.checkpoint();
                    s.dir_cleanup.?.finishContents();
                    s.dir_cleanup = null;
                    s.directory = .{};
                    s.source_phase = 1;
                    return false;
                }
                if (s.source_phase == 1) {
                    if (s.writer_cleanup == null) s.writer_cleanup = .init(&s.writer);
                    var steps: usize = 1;
                    if (!s.writer_cleanup.?.step(self.allocator, &steps)) return false;
                    if (original) |port| try port.checkpoint();
                    s.writer_cleanup.?.finishContents();
                    s.writer_cleanup = null;
                    s.writer = .{};
                    s.source_phase = 2;
                    return false;
                }
                if (s.source_phase == 2) {
                    if (s.ledger_cleanup == null) s.ledger_cleanup = .init(s.obsolete);
                    var steps: usize = 1;
                    if (!s.ledger_cleanup.?.step(self.allocator, &steps)) return false;
                    s.ledger_cleanup = null;
                    s.obsolete = .{};
                    s.source_phase = 3;
                    return false;
                }
                if (s.source_seal) |seal| seal.release();
                s.source_seal = null;
                if (self.published) {
                    if (!self.returned_ready.load(.acquire)) return false;
                    if (!try self.rootsStep(&self.returned, s, original)) return false;
                }
                if (self.refs.load(.acquire) != 1) return false;
                self.state_retiring = true;
            }
            const bytes = std.mem.asBytes(s);
            const end = @min(bytes.len, self.state_offset + 16 * 1024);
            @memset(bytes[self.state_offset..end], undefined);
            self.state_offset = end;
            if (end != bytes.len) return false;
            self.state = null;
            self.allocator.rawFree(bytes, .of(State), @returnAddress());
            return false;
        }
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Candidate) <= 16 * 1024);
        const self: *Candidate = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.state == null and self.refs.load(.acquire) == 1);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
