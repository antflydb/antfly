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

//! Registered physical checkpoint completion over independently pinned metadata.
//! The caller reserves the ID/publication lane and quiesces root mutation until
//! all physical work joins. Descriptor permission first fences actual Backend
//! recovery. This owner never acknowledges WAL or installs durable Backend roots.
const std = @import("std");
const resource = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const io = @import("storage_io.zig");
const repository = @import("repository.zig");
const manifest = @import("../lsm/manifest.zig");
const Directory = @import("run_directory.zig").Directory;
const Ledger = @import("obsolete_ledger.zig").Ledger;
const Publication = @import("registered_publication.zig");
const Cleanup = @import("output_cleanup.zig");
const slice = .lsm_table_builder_working_set;

const Source = struct {
    cursor: Directory.Cursor,
    ledger: Ledger.Cursor,
    count: usize,
    root: []const u8,
    pub fn runCount(self: *Source) usize {
        return self.cursor.directory.count();
    }
    pub fn obsoleteCount(self: *Source) usize {
        return self.count;
    }
    pub fn nextRun(self: *Source) ?manifest.RunMeta {
        var meta = repository.runMeta((self.cursor.next() orelse return null).run.*);
        meta.path = repository.manifestRelativePath(self.root, meta.path);
        return meta;
    }
    pub fn nextObsolete(self: *Source) ?manifest.ObsoletePathMeta {
        const entry = self.ledger.next() orelse return null;
        return .{ .path = repository.manifestRelativePath(self.root, entry.path), .delete_after_ns = entry.delete_after_ns };
    }
};
const Physical = @import("owned_manifest_set.zig").Publication(*Source);
const Body = struct {
    directory: Directory = .{},
    ledger: Ledger = .{},
    source: Source = undefined,
    physical: Physical = undefined,
    root: [std.fs.max_path_bytes]u8 = undefined,
    root_len: usize,
};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    manager: *resource.ResourceManager,
    host: resource.AllocatorHost,
    queue: *Cleanup.Queue,
    registration: resource.PayloadRetirement = .{},
    credit: ?resource.Reservation = null,
    grant_credit: ?resource.Reservation = null,
    grant: ?io.DurabilityResources = null,
    body: ?*Body = null,
    body_offset: usize = 0,
    directory_cleanup: ?Directory.Reclaimer = null,
    ledger_cleanup: ?Ledger.Reclaimer = null,
    metadata_phase: usize = 0,
    receipt: ?Publication.Candidate.PreparedReceipt = null,
    lineage: repository.ManifestJournal.CheckpointFence,
    id: u64,
    next_run_id: u64,
    creations: [2]?Cleanup.Creation = @splat(null),
    tickets: [2]?*Cleanup.Ticket = @splat(null),
    ticket_index: usize = 0,
    cleanup_index: usize = 0,
    physical_live: bool = false,
    prepared: bool = false,
    descriptor_permitted: bool = false,
    durable: bool = false,
    closing: bool = false,
    complete: bool = false,

    /// Captures immutable forks before returning. The prepared frame remains a
    /// synchronous borrow only until descriptor permission or requestRetirement.
    /// Root/lineage bytes are copied; no Backend, Storage or Port is retained.
    pub fn start(a: std.mem.Allocator, manager: *resource.ResourceManager, queue: *Cleanup.Queue, frame: *Publication.Candidate, root: []const u8, id: u64, next_run_id: u64, lineage: *const repository.ManifestJournal.CheckpointFence, scope: admission.Port) !*Owner {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (root.len == 0 or root.len + 96 >= std.fs.max_path_bytes) return error.NameTooLong;
        if (id == 0 or next_run_id <= id) return error.InvalidArgument;
        if (frame.manager != manager or queue.manager != manager or frame.allocator.ptr != a.ptr or frame.allocator.vtable != a.vtable or queue.allocator.ptr != a.ptr or queue.allocator.vtable != a.vtable) return error.ResourceAccountingMismatch;
        const receipt = try frame.preparedReceiptWithAdmission(original);
        const roots = try frame.preparedRootsWithAdmission(original);
        if (roots.directory == null or roots.directory.?.memory_run_count != 0) return error.InvalidArgument;
        try queue.retainProducerWithAdmission(original);
        var queue_retained = true;
        defer if (queue_retained) queue.releaseProducer();
        var host: ?resource.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(slice, @sizeOf(Owner) + @sizeOf(Body), original);
        defer construction.retire();
        const self = try a.create(Owner);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .queue = queue, .receipt = receipt, .lineage = lineage.*, .id = id, .next_run_id = next_run_id };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        queue_retained = false;
        errdefer self.requestRetirement();
        const body = try a.create(Body);
        body.* = .{ .root_len = root.len };
        self.body = body;
        @memcpy(body.root[0..root.len], root);
        try frame.validatePreparedReceiptFixedWithAdmission(receipt, original);
        body.directory = roots.directory.?.forkContents();
        body.ledger = roots.obsolete.fork();
        body.source = .{ .cursor = body.directory.readCursor(), .ledger = body.ledger.iterator(), .count = body.ledger.count(), .root = body.root[0..root.len] };
        return self;
    }

    /// Stops before opening even the descriptor temporary. All path tickets
    /// precede physical creation and the same funded grant serves all files.
    pub fn prepareWithAdmission(self: *Owner, storage: io.Storage, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.descriptor_permitted) return error.InvalidArgument;
        if (self.prepared) return;
        const body = self.body.?;
        if (!self.physical_live) {
            if (self.grant_credit == null) self.grant_credit = try self.manager.reserveWithoutReclaimWithAdmission(slice, try storage.appendResourceBytes(0), original);
            if (self.grant == null) self.grant = (try storage.tryPrepareDurabilityWithAdmission(original)) orelse return error.ResourceBudgetExceeded;
            try body.physical.initInPlace(self.grant.?, body.root[0..body.root_len], self.id, 0, self.next_run_id, &body.source, repository.maxManifestReadBytes());
            self.physical_live = true;
            self.grant = null;
            for (&self.creations, 0..) |*creation, index| creation.* = Cleanup.Creation.init(self.queue, body.physical.paths[index][0..body.physical.path_lens[index]]);
        }
        while (self.ticket_index < 2) {
            const index = self.ticket_index;
            try self.creations[index].?.resumeWithAdmission(original);
            self.tickets[index] = try self.creations[index].?.take();
            self.ticket_index += 1;
        }
        try body.physical.prepareDescriptorWithAdmission(original);
        self.prepared = true;
    }

    /// Caller holds backend.mu and the reserved publication lane, with root
    /// mutation quiesced. Fence recovery before any descriptor IO can begin.
    /// No Backend pointer escapes into this physical completion owner.
    pub fn permitDescriptorWithAdmission(self: *Owner, backend: anytype, frame: *Publication.Candidate, original: admission.Port) !void {
        try original.checkLifetime();
        if (self.closing or !self.prepared or self.descriptor_permitted) return error.InvalidArgument;
        if (backend.options.resource_manager != self.manager or backend.allocator.ptr != self.allocator.ptr or backend.allocator.vtable != self.allocator.vtable or backend.options.unpublished_outputs != self.queue) return error.ResourceAccountingMismatch;
        if (backend.closing.load(.acquire)) return error.BackendClosing;
        if (backend.options.backend.read_only) return error.ReadOnly;
        const body = self.body.?;
        const root = backend.root_dir orelse return error.ExecutionWaitUnsupported;
        if (!std.mem.eql(u8, root, body.root[0..body.root_len]) or !backend.manifest_publish_in_flight or !backend.manifest_checkpoint_build_in_flight or backend.manifest_checkpoint_build_id != self.id or backend.next_run_id != self.next_run_id) return error.CompactionPlanningStale;
        try frame.validatePreparedReceiptFixedWithAdmission(self.receipt.?, original);
        try backend.validateCapturedPublicationWithAdmission(frame, &backend.registered_publication, original);
        if (!self.lineage.matches(backend)) return error.RecoveryRequired;
        backend.manifest_recovery_required = true;
        backend.manifest_journal.sequence = null;
        backend.markManifestDirty();
        self.descriptor_permitted = true;
        self.receipt = null;
        self.lineage = undefined;
    }

    pub fn finishWithAdmission(self: *Owner, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or !self.descriptor_permitted) return error.InvalidArgument;
        if (self.durable) return;
        try self.body.?.physical.resumeWithAdmission(original);
        // Fixed physical completion receipt. Backend durable installation and
        // WAL acknowledgment remain separate, and recovery stays fenced.
        self.durable = true;
    }
    pub const Completion = struct { descriptor: repository.manifest_set.Descriptor, next_run_id: u64, checkpoint_bytes: u64 };
    pub fn completionWithAdmission(self: *Owner, original: admission.Port) !Completion {
        try original.checkLifetime();
        if (self.closing or !self.durable) return error.InvalidArgument;
        return .{ .descriptor = .{ .checkpoint = self.id, .first_segment = self.id, .sequence = 0 }, .next_run_id = self.next_run_id, .checkpoint_bytes = self.body.?.physical.encoder.total };
    }
    /// Final caller access. Caller joins registered retirement before releasing
    /// its Backend publication reservation or storage-provider lifetime.
    pub fn requestRetirement(self: *Owner) void {
        if (self.closing) return;
        self.closing = true;
        self.receipt = null;
        self.lineage = undefined;
        self.registration.publish();
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.physical_live) {
            if (original) |port| try self.body.?.physical.abortWithAdmission(port);
            self.body.?.physical.deinit();
            self.physical_live = false;
        }
        if (self.grant) |*grant| {
            if (original) |port| try grant.retireWithAdmission(port) else grant.deinit();
            self.grant = null;
        }
        if (self.grant_credit) |*credit| credit.release();
        self.grant_credit = null;
        if (self.cleanup_index < 2) {
            const index = self.cleanup_index;
            if (self.tickets[index]) |ticket| {
                // After permission, recovery owns both final paths regardless
                // of cancellation or descriptor completion uncertainty.
                if (self.descriptor_permitted) ticket.destroy() else if (original) |port| try ticket.abandonWithAdmission(port) else ticket.abandon();
                self.tickets[index] = null;
            }
            if (self.creations[index]) |*creation| try creation.retireWithAdmission(original);
            self.creations[index] = null;
            self.cleanup_index += 1;
            return false;
        }
        if (self.body) |body| {
            if (self.metadata_phase == 0) {
                if (self.directory_cleanup == null) self.directory_cleanup = .init(&body.directory);
                var steps: usize = 1;
                if (!self.directory_cleanup.?.step(self.allocator, &steps)) return false;
                if (original) |port| try port.checkpoint();
                self.directory_cleanup.?.finishContents();
                self.directory_cleanup = null;
                body.directory = .{};
                self.metadata_phase = 1;
                return false;
            }
            if (self.metadata_phase == 1) {
                if (self.ledger_cleanup == null) self.ledger_cleanup = .init(body.ledger);
                var steps: usize = 1;
                if (!self.ledger_cleanup.?.step(self.allocator, &steps)) return false;
                self.ledger_cleanup = null;
                body.ledger = .{};
                self.metadata_phase = 2;
                return false;
            }
            const bytes = std.mem.asBytes(body);
            const end = @min(bytes.len, self.body_offset + 16 * 1024);
            @memset(bytes[self.body_offset..end], undefined);
            self.body_offset = end;
            if (end != bytes.len) return false;
            self.allocator.rawFree(bytes, .of(Body), @returnAddress());
            self.body = null;
        }
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Owner) <= 16 * 1024);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.body == null and self.grant == null);
        const a = self.allocator;
        const host = self.host;
        const queue = self.queue;
        a.destroy(self);
        queue.releaseProducer();
        host.release();
    }
};
