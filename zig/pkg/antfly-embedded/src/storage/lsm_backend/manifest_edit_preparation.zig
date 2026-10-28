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

//! Funded hot-journal preparation over immutable captured roots. Caller holds
//! the manifest lane through IO and installation. No execution borrow is stored;
//! arrays remain owned on rejection and retire independently in physical chunks.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const Ledger = @import("obsolete_ledger.zig").Ledger;
const Snapshot = @import("ledger_reclamation.zig").Snapshot;
const repository = @import("repository.zig");
pub const manifest = @import("../lsm/manifest.zig");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const execution = @import("../../common/background_execution.zig");
const Comparison = @import("run_comparison.zig").Cursor;
const platform = @import("antfly_platform");
const Run = repository.Run;
const storage_io = @import("storage_io.zig");
pub const checkpoint_publication = @import("normal_checkpoint_publication.zig");
pub var test_hook: ?*const fn (admission.Port) anyerror!void = null;
pub var test_append_hook: ?*const fn (*Frame, admission.Port) anyerror!void = null;
pub var test_append_join_hook: ?*const fn (admission.Port) anyerror!void = null;
pub var test_retirement_hook: ?*const fn (admission.Port) anyerror!void = null;

pub const Frame = struct {
    credit: resources.Reservation,
    append_grant: ?storage_io.DurabilityResources = null,
    append_pending: ?storage_io.SyncOperation = null,
    append_started: bool = false,
    append_phase: enum { prepare, append, sync, complete } = .prepare,
    append_path: [std.fs.max_path_bytes]u8 = undefined,
    append_path_len: usize = 0,
    deadline_ns: u64 = 0,
    checkpoint_owner: ?*checkpoint_publication.Owner = null,
    checkpoint_retirement_offset: usize = 0,
    next: ?*Frame = null,
    current: ?*Directory = null,
    previous: ?*Directory = null,
    obsolete: ?*Snapshot = null,
    previous_obsolete: ?*Snapshot = null,
    added: []manifest.RunMeta = &.{},
    removed: []u64 = &.{},
    added_paths: []manifest.ObsoletePathMeta = &.{},
    removed_paths: [][]const u8 = &.{},
    encoded: []u8 = &.{},
    counts: [4]usize = @splat(0),
    wire: usize = 68,
    retirement_field: usize = 0,
    retirement_offset: usize = 0,

    pub fn create(backend: anytype) !*Frame {
        var credit = try backend.options.resource_manager.?.reserve(.lsm_table_builder_working_set, @sizeOf(Frame) + 2 * @sizeOf(Directory) + 2 * @sizeOf(Snapshot));
        errdefer credit.release();
        const self = try backend.allocator.create(Frame);
        self.* = .{ .credit = credit };
        return self;
    }
    pub fn capture(self: *Frame, backend: anytype) !void {
        self.current = try backend.run_directory.?.fork(backend.allocator);
        self.previous = try backend.manifest_directory.?.fork(backend.allocator);
        self.obsolete = try Snapshot.capture(backend, &backend.obsolete_paths);
        self.previous_obsolete = try Snapshot.capture(backend, &backend.manifest_journal.obsolete);
    }
    fn cancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const backend: *const @import("../lsm_backend.zig").Backend = @ptrCast(@alignCast(raw.?));
        return @intFromBool(backend.closing.load(.acquire));
    }
    fn allocate(comptime T: type, allocator: std.mem.Allocator, count: usize, original: admission.Port) ![]T {
        try original.checkpoint();
        if (count == 0) return &.{};
        const bytes = try std.math.mul(usize, count, @sizeOf(T));
        // Filling and retirement have their own physical checkpoints. Avoid
        // Allocator.alloc's whole-buffer Debug poison before those cursors run.
        const raw = allocator.rawAlloc(bytes, .of(T), @returnAddress()) orelse return error.OutOfMemory;
        return @as([*]T, @ptrCast(@alignCast(raw)))[0..count];
    }
    pub fn prepare(self: *Frame, backend: anytype, owner: *@import("../background_runtime.zig").BackendRuntime, root: []const u8, sequence: u64, next_id: u64) !void {
        const io = owner.io() orelse return error.ExecutionWaitUnsupported;
        const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
        self.deadline_ns = deadline;
        {
            var pass = try execution.Pass.init(owner.runnableOwner(), &io, deadline, .{ .context = backend, .is_cancelled = cancelled });
            defer pass.deinit();
            var scope = try pass.admissionScope(io);
            const payload = scope.payload();
            try self.walk(root, false, .init(&payload));
        }
        // Resource-manager growth occurs after releasing CPU, outside every
        // service guard. The same deadline governs the subsequent encoding pass.
        if (self.wire > repository.maxManifestReadBytes()) return error.ManifestEditTooLarge;
        var bytes: u64 = self.wire;
        inline for (.{ manifest.RunMeta, u64, manifest.ObsoletePathMeta, []const u8 }, 0..) |T, i| bytes = try std.math.add(u64, bytes, try std.math.mul(u64, self.counts[i], @sizeOf(T)));
        try self.credit.growBoundedOversized(bytes, 1);
        var pass = try execution.Pass.init(owner.runnableOwner(), &io, deadline, .{ .context = backend, .is_cancelled = cancelled });
        defer pass.deinit();
        var scope = try pass.admissionScope(io);
        const payload = scope.payload();
        const original = admission.Port.init(&payload);
        try original.checkpoint();
        self.added = try allocate(manifest.RunMeta, backend.allocator, self.counts[0], original);
        self.removed = try allocate(u64, backend.allocator, self.counts[1], original);
        self.added_paths = try allocate(manifest.ObsoletePathMeta, backend.allocator, self.counts[2], original);
        self.removed_paths = try allocate([]const u8, backend.allocator, self.counts[3], original);
        try self.walk(root, true, original);
        self.encoded = try allocate(u8, backend.allocator, self.wire, original);
        if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(original);
        try manifest.encodeJournalFrameIntoWithAdmission(self.encoded, sequence, false, self.removed, self.removed_paths, .{ .next_run_id = next_id, .runs = self.added, .obsolete_paths = self.added_paths }, original);
    }
    fn observeAppend(self: *Frame, original: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        self.append_pending.?.waitWithAdmission(trace.bind(original).?) catch |cause| {
            if (trace.failure) |rejected| return rejected;
            self.append_pending.?.deinit();
            self.append_pending = null;
            return cause;
        };
        self.append_pending.?.deinit();
        self.append_pending = null;
    }
    fn driveAppend(self: *Frame, backend: anytype, root: []const u8, segment: u64, original: admission.Port) !void {
        self.append_path_len = (try std.fmt.bufPrint(&self.append_path, "{s}/manifest-{d}.journal", .{ root, segment })).len;
        self.append_grant = (try backend.storage.?.tryPrepareAppendWithAdmission(self.encoded, original)) orelse return error.ResourceBudgetExceeded;
        if (comptime @import("builtin").is_test) if (test_append_hook) |hook| try hook(self, original);
        self.append_pending = try self.append_grant.?.beginPreparedAppend(self.append_path[0..self.append_path_len], false, original);
        self.append_started = true;
        self.append_phase = .append;
        if (comptime @import("builtin").is_test) if (test_append_hook) |hook| try hook(self, original);
        try self.observeAppend(original);
        self.append_pending = try self.append_grant.?.beginSyncContents(self.append_path[0..self.append_path_len], original);
        self.append_phase = .sync;
        if (comptime @import("builtin").is_test) if (test_append_hook) |hook| try hook(self, original);
        try self.observeAppend(original);
        self.append_phase = .complete;
    }
    fn joinAppendIndependently(self: *Frame, owner: *@import("../background_runtime.zig").BackendRuntime) !void {
        const io = owner.io() orelse return error.ExecutionWaitUnsupported;
        // An accepted append keeps the publication lane until physical join.
        // Each refusal releases cleanup CPU before its retry/backoff; ordinary
        // closure does not revoke this protected completion authority.
        while (self.append_pending != null) {
            const attempt = blk: {
                var pass = execution.Pass.initWithOptions(owner.runnableOwner(), &io, platform.time.monotonicNs() +| 100 * std.time.ns_per_ms, .{}, .{ .allow_wait = false, .cleanup = true }) catch |cause| break :blk @as(anyerror!void, cause);
                defer pass.deinit();
                var scope = pass.admissionScope(io) catch |cause| break :blk @as(anyerror!void, cause);
                const payload = scope.payload();
                const original = admission.Port.init(&payload);
                if (comptime @import("builtin").is_test) if (test_append_join_hook) |hook| hook(original) catch |cause| break :blk @as(anyerror!void, cause);
                break :blk self.observeAppend(original);
            };
            attempt catch |cause| {
                if (self.append_pending == null) return; // Physical failure was joined.
                switch (cause) {
                    error.Timeout, error.DeadlineExceeded, error.ResourceBudgetExceeded, error.ParentMemoryBusy, error.CapacityUnavailable, error.OutOfMemory, error.SchedulerFull, error.ResourceTemporarilyUnavailable, error.ConcurrencyUnavailable => try io.sleep(.fromMilliseconds(1), .awake),
                    else => return cause,
                }
            };
        }
    }
    pub fn appendWithExecution(self: *Frame, backend: anytype, owner: *@import("../background_runtime.zig").BackendRuntime, root: []const u8, segment: u64) !void {
        const io = owner.io() orelse return error.ExecutionWaitUnsupported;
        try self.credit.growBoundedOversized(try backend.storage.?.appendResourceBytes(self.encoded.len), 1);
        const written = blk: {
            var pass = execution.Pass.init(owner.runnableOwner(), &io, self.deadline_ns, .{ .context = backend, .is_cancelled = cancelled }) catch |cause| break :blk @as(anyerror!void, cause);
            defer pass.deinit();
            var scope = pass.admissionScope(io) catch |cause| break :blk @as(anyerror!void, cause);
            const payload = scope.payload();
            break :blk self.driveAppend(backend, root, segment, .init(&payload));
        };
        written catch |cause| {
            if (self.append_pending != null) self.joinAppendIndependently(owner) catch {};
            return cause;
        };
    }
    pub fn prepareCheckpoint(self: *Frame, backend: anytype, owner: *@import("../background_runtime.zig").BackendRuntime, root: []const u8, id: u64, next_id: u64) !void {
        const io = owner.io() orelse return error.ExecutionWaitUnsupported;
        const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
        // Fixed streaming scratch plus one retained provider grant, independent
        // of metadata/key count. Funding occurs off-lock before CPU admission.
        try self.credit.growBoundedOversized(try std.math.add(u64, @sizeOf(checkpoint_publication.Owner), try backend.storage.?.appendResourceBytes(0)), 1);
        var pass = try execution.Pass.init(owner.runnableOwner(), &io, deadline, .{ .context = backend, .is_cancelled = cancelled });
        defer pass.deinit();
        var scope = try pass.admissionScope(io);
        const payload = scope.payload();
        const original = admission.Port.init(&payload);
        var runs = self.current.?.readCursor();
        var prior: ?Run = null;
        while (runs.next()) |handle| {
            if (prior) |previous| try validate(&.{ previous, handle.run.* }, original) else try validate(&.{handle.run.*}, original);
            prior = handle.run.*;
        }
        self.checkpoint_owner = &(try allocate(checkpoint_publication.Owner, backend.allocator, 1, original))[0];
        const checkpoint = self.checkpoint_owner.?;
        checkpoint.* = .{ .deadline = deadline };
        try checkpoint.prepare(backend.storage.?, backend.options.unpublished_outputs.?, root, id, next_id, .{
            .cursor = self.current.?.readCursor(),
            .ledger = self.obsolete.?.value.iterator(),
            .count = self.obsolete.?.value.count(),
            .root = root,
        }, original);
    }
    pub fn finishCheckpoint(self: *Frame, backend: anytype, owner: *@import("../background_runtime.zig").BackendRuntime) !usize {
        const io = owner.io() orelse return error.ExecutionWaitUnsupported;
        var pass = try execution.Pass.init(owner.runnableOwner(), &io, self.checkpoint_owner.?.deadline, .{ .context = backend, .is_cancelled = cancelled });
        defer pass.deinit();
        var scope = try pass.admissionScope(io);
        const payload = scope.payload();
        return self.checkpoint_owner.?.finish(.init(&payload));
    }
    fn bound(a_ns: ?[]const u8, a: []const u8, b_ns: ?[]const u8, b: []const u8, original: admission.Port) !std.math.Order {
        var comparison = Comparison.bound(a_ns, a, b_ns, b);
        return comparison.resumeWithAdmission(original);
    }
    fn validate(runs: []const Run, original: admission.Port) !void {
        var prior: ?Run = null;
        for (runs) |run| {
            try original.checkpoint();
            if (run.path == null) return error.RunStateUnavailable;
            if (run.entry_count == 0 or try bound(run.smallest_namespace_name, run.smallest_key, run.largest_namespace_name, run.largest_key, original) == .gt) return error.InvalidTableFile;
            if (prior) |previous| {
                if (previous.level > run.level) return error.InvalidTableFile;
                if (previous.level == run.level) {
                    if (run.level == 0) {
                        const a = if (previous.visibility_id == 0) previous.id else previous.visibility_id;
                        const b = if (run.visibility_id == 0) run.id else run.visibility_id;
                        if (a < b or (a == b and try bound(previous.largest_namespace_name, previous.largest_key, run.smallest_namespace_name, run.smallest_key, original) != .lt)) return error.InvalidTableFile;
                    } else if (try bound(previous.smallest_namespace_name, previous.smallest_key, run.smallest_namespace_name, run.smallest_key, original) == .gt or
                        try bound(previous.largest_namespace_name, previous.largest_key, run.smallest_namespace_name, run.smallest_key, original) != .lt) return error.InvalidTableFile;
                }
            }
            prior = run;
        }
    }
    fn walk(self: *Frame, root: []const u8, fill: bool, original: admission.Port) !void {
        var counts: [4]usize = @splat(0);
        var changes = Directory.ChangeCursor.init(self.previous.?, self.current.?);
        while (!changes.done()) {
            var credits: usize = 64;
            const change = (try changes.nextWithAdmission(&credits, original)) orelse continue;
            if (change.kind == .remove) {
                if (fill) self.removed[counts[1]] = change.run.id else self.wire = try std.math.add(usize, self.wire, 8);
                counts[1] += 1;
            } else {
                var search = Directory.ReadSearch.init(self.current.?.tree.root, .{ .run = change.run });
                try search.resumeWithAdmission(original);
                const start = search.rank -| 1;
                const end = @min(self.current.?.count(), search.rank + 2);
                var neighbors: [3]Run = undefined;
                for (start..end, 0..) |rank, i| neighbors[i] = self.current.?.tree.root.?.at(rank).run.*;
                try validate(neighbors[0 .. end - start], original);
                var meta = repository.runMeta(change.run.*);
                meta.path = repository.manifestRelativePath(root, meta.path);
                if (fill) self.added[counts[0]] = meta else {
                    self.wire = try std.math.add(usize, self.wire, 112);
                    for ([_][]const u8{ meta.path, meta.smallest_namespace_name orelse &.{}, meta.smallest_key, meta.largest_namespace_name orelse &.{}, meta.largest_key }) |bytes| self.wire = try std.math.add(usize, self.wire, bytes.len);
                }
                counts[0] += 1;
            }
        }
        var paths = Ledger.ChangeCursor.init(&self.previous_obsolete.?.value, &self.obsolete.?.value);
        while (!paths.done()) {
            var credits: usize = 64;
            const change = (try paths.nextWithAdmission(&credits, original)) orelse continue;
            const path = repository.manifestRelativePath(root, change.path.path);
            if (change.kind == .remove) {
                if (fill) self.removed_paths[counts[3]] = path else self.wire = try std.math.add(usize, self.wire, try std.math.add(usize, 4, path.len));
                counts[3] += 1;
            } else {
                if (fill) self.added_paths[counts[2]] = .{ .path = path, .delete_after_ns = change.path.delete_after_ns } else self.wire = try std.math.add(usize, self.wire, try std.math.add(usize, 12, path.len));
                counts[2] += 1;
            }
        }
        if (!fill) self.counts = counts else std.debug.assert(std.meta.eql(self.counts, counts));
    }
    pub fn accountedMemoryBytes(self: *const Frame, pass: u64) u64 {
        // Header storage is already funded by the retained builder reservation.
        // Sources can retain older COW accounts after live roots have moved on.
        var bytes: u64 = 0;
        if (self.current) |directory| bytes +|= directory.accountedMemoryBytes(pass) -| @sizeOf(Directory);
        if (self.previous) |directory| bytes +|= directory.accountedMemoryBytes(pass) -| @sizeOf(Directory);
        return bytes;
    }
    /// Return all source pins before byte-only retirement can run. Descriptors
    /// may contain old borrowed addresses; cleanup never dereferences them.
    pub fn detachLocked(self: *Frame, backend: anytype) void {
        if (self.current) |directory| backend.retireCheckpointDirectory(directory);
        self.current = null;
        if (self.previous) |directory| backend.retireCheckpointDirectory(directory);
        self.previous = null;
        if (self.obsolete) |snapshot| snapshot.retire(backend);
        self.obsolete = null;
        if (self.previous_obsolete) |snapshot| snapshot.retire(backend);
        self.previous_obsolete = null;
    }
    pub fn stepWithAdmission(self: *Frame, a: std.mem.Allocator, credits: *usize, original: ?admission.Port) !bool {
        if (comptime @import("builtin").is_test) if (original) |port| if (test_retirement_hook) |hook| try hook(port);
        if (self.append_pending != null) {
            if (original) |port| self.observeAppend(port) catch |cause| {
                if (self.append_pending != null) return cause;
            } else {
                self.append_pending.?.deinit();
                self.append_pending = null;
            }
        }
        if (self.append_grant) |*grant| {
            if (original) |port| try grant.retireWithAdmission(port) else grant.deinit();
            self.append_grant = null;
        }
        if (self.checkpoint_owner) |checkpoint| {
            if (credits.* == 0) return false;
            credits.* -= 1;
            if (self.checkpoint_retirement_offset == 0 and !try checkpoint.retireStep(original)) return false;
            const bytes = std.mem.asBytes(checkpoint);
            while (self.checkpoint_retirement_offset < bytes.len) {
                if (credits.* == 0) return false;
                if (original) |port| try port.checkpoint();
                credits.* -= 1;
                const end = @min(bytes.len, self.checkpoint_retirement_offset + 16 * 1024);
                @memset(bytes[self.checkpoint_retirement_offset..end], undefined);
                self.checkpoint_retirement_offset = end;
            }
            a.rawFree(bytes, .of(checkpoint_publication.Owner), @returnAddress());
            self.checkpoint_owner = null;
        }
        inline for (.{ "added", "removed", "added_paths", "removed_paths", "encoded" }, 0..) |field, i| {
            if (self.retirement_field == i) {
                const T = @typeInfo(@TypeOf(@field(self, field))).pointer.child;
                const bytes = std.mem.sliceAsBytes(@field(self, field));
                while (self.retirement_offset < bytes.len) {
                    if (credits.* == 0) return false;
                    if (original) |port| try port.checkpoint();
                    credits.* -= 1;
                    const end = @min(bytes.len, self.retirement_offset + 16 * 1024);
                    @memset(bytes[self.retirement_offset..end], undefined);
                    self.retirement_offset = end;
                }
                if (bytes.len != 0) a.rawFree(bytes, .of(T), @returnAddress());
                @field(self, field) = &.{};
                self.retirement_field += 1;
                self.retirement_offset = 0;
            }
        }
        return true;
    }
    pub fn finish(self: *Frame, a: std.mem.Allocator) void {
        var credit = self.credit;
        a.destroy(self);
        credit.release();
    }
};
