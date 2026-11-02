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
//! recovery. Durable Backend views may retain its immutable metadata after IO joins.
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
    root: [std.fs.max_path_bytes]u8 = undefined,
    root_len: usize,
};

/// Caller-prepaid stable observation of this checkpoint's physical retirement.
/// Keep it alive until completion or explicit detach while the creator is live.
/// This joins only the issuing checkpoint's IO/grants/tickets, not its borrowed
/// metadata, all users of a storage provider, or the manager's accounting debt.
pub const PhysicalJoin = struct {
    const Identity = @import("read_generation.zig").Identity;
    const State = enum(u8) { unbound, pending, complete };
    issuer: ?*Owner = null,
    identity: Identity = .{},
    state: std.atomic.Value(State) = .init(.unbound),

    pub const Receipt = struct {
        join: *PhysicalJoin,
        issuer: *Owner,
        identity: Identity,
        fn validate(self: @This()) !void {
            if (!self.identity.valid() or self.join.issuer != self.issuer or !std.meta.eql(self.identity, self.join.identity) or self.join.state.load(.acquire) == .unbound) return error.InvalidArgument;
        }
        /// Fixed, non-yielding observation, safe under a caller's metadata lock.
        /// Reads only caller-owned storage, even after issuer exit.
        pub fn completedWithAdmission(self: @This(), original: admission.Port) !bool {
            try original.checkLifetime();
            try self.validate();
            return self.join.state.load(.acquire) == .complete;
        }
        /// Pending waits require the original deadline and execution-wait
        /// authority. Refusal retains this observation and all physical work.
        pub fn waitWithAdmission(self: @This(), original: admission.Port) !void {
            if (try self.completedWithAdmission(original)) return;
            _ = (try original.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
            _ = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
            while (!try self.completedWithAdmission(original)) try original.waitForProgress();
        }
        /// No issuer access. Caller serializes receipt reads/reset/reuse.
        pub fn resetWithAdmission(self: @This(), original: admission.Port) !void {
            if (!try self.completedWithAdmission(original)) return error.ParentMemoryBusy;
            self.join.reset();
        }
    };
    fn reset(self: *PhysicalJoin) void {
        self.issuer = null;
        self.identity = .{};
        self.state.store(.unbound, .release);
    }
};

pub const Owner = struct {
    pub const PhysicalRetirement = PhysicalJoin;
    allocator: std.mem.Allocator,
    manager: *resource.ResourceManager,
    host: resource.AllocatorHost,
    queue: *Cleanup.Queue,
    registration: resource.PayloadRetirement = .{},
    credit: ?resource.Reservation = null,
    grant_credit: ?resource.Reservation = null,
    grant: ?io.DurabilityResources = null,
    body: ?*Body = null,
    physical: ?*Physical = null,
    physical_offset: usize = 0,
    queue_retained: bool = true,
    metadata_borrowed: std.atomic.Value(bool) = .init(false),
    installed: bool = false,
    covered_logical_bytes: u64 = 0,
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
    physical_retiring: bool = false,
    physical_retired: bool = false,
    physical_join: ?*PhysicalJoin = null,
    checkpoint_bytes: u64 = 0,
    prepared: bool = false,
    descriptor_permitted: bool = false,
    durable: bool = false,
    closing: bool = false,
    complete: bool = false,

    /// Captures immutable forks before returning. The prepared frame remains a
    /// identity receipt until installation or requestRetirement. Metadata forks
    /// remain independently owned if the original frame exits without installation.
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
        var construction = try manager.reserveConstructionWithAdmission(slice, @sizeOf(Owner) + @sizeOf(Body) + @sizeOf(Physical), original);
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
        if (self.closing or self.physical_retiring or self.descriptor_permitted) return error.InvalidArgument;
        if (self.prepared) return;
        const body = self.body.?;
        if (!self.physical_live) {
            if (self.physical == null) self.physical = try self.allocator.create(Physical);
            if (self.grant_credit == null) self.grant_credit = try self.manager.reserveWithoutReclaimWithAdmission(slice, try storage.appendResourceBytes(0), original);
            if (self.grant == null) self.grant = (try storage.tryPrepareDurabilityWithAdmission(original)) orelse return error.ResourceBudgetExceeded;
            try self.physical.?.initInPlace(self.grant.?, body.root[0..body.root_len], self.id, 0, self.next_run_id, &body.source, repository.maxManifestReadBytes());
            self.physical_live = true;
            self.grant = null;
            for (&self.creations, 0..) |*creation, index| creation.* = Cleanup.Creation.init(self.queue, self.physical.?.paths[index][0..self.physical.?.path_lens[index]]);
        }
        while (self.ticket_index < 2) {
            const index = self.ticket_index;
            try self.creations[index].?.resumeWithAdmission(original);
            self.tickets[index] = try self.creations[index].?.take();
            self.ticket_index += 1;
        }
        try self.physical.?.prepareDescriptorWithAdmission(original);
        self.prepared = true;
    }

    /// Caller holds backend.mu and the reserved publication lane, with root
    /// mutation quiesced. Fence recovery before any descriptor IO can begin.
    /// No Backend pointer escapes into this physical completion owner.
    pub fn permitDescriptorWithAdmission(self: *Owner, backend: anytype, frame: *Publication.Candidate, original: admission.Port) !void {
        try original.checkLifetime();
        if (self.closing or self.physical_retiring or !self.prepared or self.descriptor_permitted) return error.InvalidArgument;
        if (backend.options.resource_manager != self.manager or backend.allocator.ptr != self.allocator.ptr or backend.allocator.vtable != self.allocator.vtable or backend.options.unpublished_outputs != self.queue) return error.ResourceAccountingMismatch;
        if (backend.closing.load(.acquire)) return error.BackendClosing;
        if (backend.options.backend.read_only) return error.ReadOnly;
        const body = self.body.?;
        const root = backend.root_dir orelse return error.ExecutionWaitUnsupported;
        if (!std.mem.eql(u8, root, body.root[0..body.root_len]) or !backend.manifest_publish_in_flight or !backend.manifest_checkpoint_build_in_flight or backend.manifest_checkpoint_build_id != self.id or backend.next_run_id != self.next_run_id) return error.CompactionPlanningStale;
        try frame.validatePreparedReceiptFixedWithAdmission(self.receipt.?, original);
        try backend.validateCapturedPublicationWithAdmission(frame, &backend.registered_publication, original);
        if (!self.lineage.matches(backend)) return error.RecoveryRequired;
        if (backend.registered_manifest_checkpoint == null and (backend.manifest_directory != null or backend.manifest_journal.obsolete.tree.account != null)) return error.ExecutionWaitUnsupported;
        self.covered_logical_bytes = backend.unpublished_wal_logical_bytes;
        backend.manifest_recovery_required = true;
        backend.manifest_journal.sequence = null;
        backend.markManifestDirty();
        self.descriptor_permitted = true;
    }

    pub fn finishWithAdmission(self: *Owner, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or !self.descriptor_permitted) return error.InvalidArgument;
        if (self.durable) return;
        if (self.physical_retiring) return error.InvalidArgument;
        try self.physical.?.resumeWithAdmission(original);
        // Fixed physical completion receipt. Backend durable installation and
        // WAL acknowledgment remain separate, and recovery stays fenced.
        self.checkpoint_bytes = self.physical.?.encoder.total;
        self.durable = true;
    }
    pub const Completion = struct { descriptor: repository.manifest_set.Descriptor, next_run_id: u64, checkpoint_bytes: u64 };
    pub fn completionWithAdmission(self: *Owner, original: admission.Port) !Completion {
        try original.checkLifetime();
        if (self.closing or !self.durable) return error.InvalidArgument;
        return .{ .descriptor = .{ .checkpoint = self.id, .first_segment = self.id, .sequence = 0 }, .next_run_id = self.next_run_id, .checkpoint_bytes = self.checkpoint_bytes };
    }
    /// Validate all receipts before a Backend changes either live or durable
    /// metadata. Caller holds its publication mutex and reservation.
    pub fn validateInstallationWithAdmission(self: *Owner, backend: anytype, frame: *Publication.Candidate, original: admission.Port) !Completion {
        try original.checkLifetime();
        if (self.closing or self.installed or !self.durable) return error.InvalidArgument;
        if (backend.options.resource_manager != self.manager or backend.allocator.ptr != self.allocator.ptr or backend.allocator.vtable != self.allocator.vtable or backend.options.unpublished_outputs != self.queue) return error.ResourceAccountingMismatch;
        if (!backend.manifest_publish_in_flight or !backend.manifest_checkpoint_build_in_flight or backend.manifest_checkpoint_build_id != self.id or backend.next_run_id != self.next_run_id) return error.CompactionPlanningStale;
        if (!self.lineage.matchesRecoveryFenced(backend)) return error.RecoveryRequired;
        try frame.validatePreparedReceiptFixedWithAdmission(self.receipt.?, original);
        try backend.validateCapturedPublicationWithAdmission(frame, &backend.registered_publication, original);
        return self.completionWithAdmission(original);
    }
    pub const Metadata = struct { directory: *Directory, ledger: Ledger };
    /// Fixed ownership receipt after coupled live-root installation succeeds.
    pub fn borrowInstalledMetadataFixed(self: *Owner) Metadata {
        std.debug.assert(self.durable and !self.closing and !self.installed);
        self.installed = true;
        self.metadata_borrowed.store(true, .release);
        self.receipt = null;
        self.lineage = undefined;
        return .{ .directory = &self.body.?.directory, .ledger = self.body.?.ledger };
    }
    /// Final Backend access, after removing every view of these headers.
    pub fn returnInstalledMetadata(self: *Owner) void {
        std.debug.assert(self.installed and self.metadata_borrowed.load(.acquire));
        self.metadata_borrowed.store(false, .release);
    }
    /// Off-lock, creator-serialized binding before publishing retirement.
    /// The actual owner mints a fresh
    /// identity; no registration or global-ready count can stand in for its join.
    pub fn observePhysicalRetirementWithAdmission(self: *Owner, join: *PhysicalJoin, original: admission.Port) !PhysicalJoin.Receipt {
        try original.checkpoint();
        if (self.closing or self.physical_retiring or self.physical_join != null or join.state.load(.acquire) != .unbound) return error.InvalidArgument;
        const identity = try @import("read_generation.zig").allocate();
        join.issuer = self;
        join.identity = identity;
        join.state.store(.pending, .release);
        self.physical_join = join;
        return .{ .join = join, .issuer = self, .identity = identity };
    }
    /// Creator-only withdrawal before physical retirement starts. Once cleanup
    /// owns the callback, its saved observer must remain through completion.
    pub fn detachPhysicalObservationWithAdmission(self: *Owner, receipt: PhysicalJoin.Receipt, original: admission.Port) !void {
        try original.checkpoint();
        try receipt.validate();
        if (self.closing or self.physical_retiring or receipt.issuer != self or self.physical_join != receipt.join) return error.InvalidArgument;
        self.physical_join = null;
        receipt.join.reset();
    }
    /// Off-lock, creator-owned terminal physical cleanup. Durable metadata/receipt remain
    /// usable for installation after the IO/grant owner has physically joined.
    pub fn retirePhysicalWithAdmission(self: *Owner, original: admission.Port) !void {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        while (!try self.retirePhysicalStep(original)) {}
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
    fn retirePhysicalStep(self: *Owner, original: ?admission.Port) !bool {
        if (original) |port| try port.checkpoint();
        if (self.physical_retired) return true;
        self.physical_retiring = true;
        if (self.physical_live) {
            if (original) |port| try self.physical.?.abortWithAdmission(port);
            self.physical.?.deinit();
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
        if (self.physical) |physical| {
            const bytes = std.mem.asBytes(physical);
            const end = @min(bytes.len, self.physical_offset + 16 * 1024);
            @memset(bytes[self.physical_offset..end], undefined);
            self.physical_offset = end;
            if (end != bytes.len) return false;
            self.allocator.rawFree(bytes, .of(Physical), @returnAddress());
            self.physical = null;
            self.credit.?.shrink(@sizeOf(Physical));
        }
        if (self.queue_retained) {
            self.queue_retained = false;
            self.queue.releaseProducer(); // Final queue access, before metadata waits.
        }
        self.physical_retired = true;
        if (self.physical_join) |join| {
            self.physical_join = null;
            join.state.store(.complete, .release); // Final observation access.
        }
        return true;
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (!try self.retirePhysicalStep(original)) return false;
        if (self.metadata_borrowed.load(.acquire)) return false;
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
        std.debug.assert(self.complete and self.body == null and self.physical == null and self.grant == null and !self.queue_retained);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
