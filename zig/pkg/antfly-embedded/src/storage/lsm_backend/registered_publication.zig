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

pub const Candidate = struct {
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
    removing: bool,
    deadline: u64 = 0,
    ready: bool = false,
    published: bool = false,
    closing: bool = false,
    complete: bool = false,

    fn validateRoots(roots: *const Roots, a: std.mem.Allocator, original: admission.Port) !void {
        try original.checkpoint();
        const seal = roots.seal orelse {
            if (roots.directory != null or roots.writer != null or roots.obsolete.tree.account != null or roots.obsolete.count() != 0 or roots.obsolete.spare.capacity != 0 or roots.obsolete.tree.spare.capacity != 0) return error.InvalidArgument;
            return;
        };
        if (seal.registration.step == null or seal.registration.step.?.advance != advance or seal.registration.context != @as(*anyopaque, @ptrCast(seal)) or seal.registration.credit == null or seal.registration.credit.?.manager != seal.manager or !seal.published or seal.complete or seal.returned_ready.load(.acquire) or seal.refs.load(.acquire) < 2 or !seal.expected.matches(roots)) return error.InvalidArgument;
        const directory = roots.directory orelse return error.InvalidArgument;
        const writer = roots.writer orelse return error.InvalidArgument;
        _ = try DirEdit.validateSource(directory, a, original);
        try WriterEdit.validateSource(writer, a, original);
        try ObsoleteEdit.validateSource(&roots.obsolete, a, original);
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
        const self = try allocate(a, manager, roots, false, 0, original);
        errdefer self.requestRetirement();
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
        const self = try allocate(a, manager, roots, true, deadline, original);
        errdefer self.requestRetirement();
        self.state.?.held = try handle.retainRegisteredSourceWithAdmission(original);
        try original.checkpoint();
        self.state.?.run = run.retainOwned();
        return self;
    }
    fn allocate(a: std.mem.Allocator, manager: *resource.ResourceManager, roots: *const Roots, removing: bool, deadline: u64, original: admission.Port) !*Candidate {
        var host: ?resource.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, @sizeOf(Candidate) + @sizeOf(State), original);
        defer construction.retire();
        const self = try a.create(Candidate);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .removing = removing, .deadline = deadline, .source_stamp = .of(roots) };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        try original.checkpoint();
        self.state = try a.create(State);
        self.state.?.* = .{};
        if (roots.seal) |seal| {
            _ = seal.refs.fetchAdd(1, .monotonic);
            self.state.?.source_seal = seal;
        }
        if (roots.directory) |directory| self.state.?.directory = directory.forkContents();
        if (roots.writer) |writer| self.state.?.writer = writer.fork();
        self.state.?.obsolete = roots.obsolete.fork();
        return self;
    }
    pub fn resumeWithAdmission(self: *Candidate, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.state == null or self.published) return error.InvalidArgument;
        if (self.ready) return;
        const s = self.state.?;
        if (!self.removing and !s.checked_id) {
            if (s.id_search == null) s.id_search = s.directory.locateId(&s.run.?);
            try s.id_search.?.resumeWithAdmission(original);
            if (s.id_search.?.matched != null) return error.DuplicateRun;
            s.id_search = null;
            s.checked_id = true;
        }
        if (s.outputs.directory == null) {
            if (s.directory_child == null) {
                if (self.removing) {
                    const lease = &s.held.?.lease.?;
                    s.directory_child = try DirEdit.startRemove(self.allocator, self.manager, &s.directory, .{ .run = lease.entry.run, .revision = lease.entry.payload.? }, original);
                } else s.directory_child = try DirEdit.startPut(self.allocator, self.manager, &s.directory, &s.held.?.lease.?, original);
            }
            try s.directory_child.?.resumeWithAdmission(original);
            s.outputs.directory = try s.directory_child.?.takeWithAdmission(original);
            s.directory_child.?.requestRetirement();
            s.directory_child = null;
        }
        if (s.outputs.writer == null) {
            if (s.writer_child == null) {
                if (self.removing) s.writer_child = try WriterEdit.startRemove(self.allocator, self.manager, &s.writer, &s.run.?, original) else s.writer_child = try WriterEdit.start(self.allocator, self.manager, &s.writer, &s.run.?, false, original);
            }
            try s.writer_child.?.resumeWithAdmission(original);
            s.outputs.writer = try s.writer_child.?.takeWithAdmission(original);
            s.writer_child.?.requestRetirement();
            s.writer_child = null;
        }
        if (self.removing and s.run.?.path != null) {
            if (s.obsolete_child == null) s.obsolete_child = try ObsoleteEdit.startPut(self.allocator, self.manager, &s.obsolete, &s.run.?, self.deadline, original);
            try s.obsolete_child.?.resumeWithAdmission(original);
            s.outputs.obsolete = try s.obsolete_child.?.takeWithAdmission(original);
            s.obsolete_child.?.requestRetirement();
            s.obsolete_child = null;
        } else s.outputs.obsolete = s.obsolete.fork();
        self.ready = true;
    }
    /// Publishes a caller-owned private working set only. Caller serializes
    /// access; the backend's durable publication and source check are separate.
    pub fn publishWithAdmission(self: *Candidate, live: *Roots, original: admission.Port) !void {
        try original.checkpoint();
        if (self.closing or self.published or !self.ready or self.state == null) return error.InvalidArgument;
        if (live.seal != self.state.?.source_seal or !self.source_stamp.matches(live)) return error.CompactionPlanningStale;
        _ = self.refs.fetchAdd(1, .monotonic); // The new working-set lease.
        self.state.?.outputs.seal = self;
        std.mem.swap(Roots, live, &self.state.?.outputs);
        self.expected = .of(live);
        self.published = true;
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
                if (!try self.rootsStep(&s.outputs, s, original)) return false;
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
