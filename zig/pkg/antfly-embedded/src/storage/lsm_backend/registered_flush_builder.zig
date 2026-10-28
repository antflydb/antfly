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

//! Retained multi-output immutable flush construction. The coordinator pins
//! immutable inputs and their vector through driving or beginAbort. Completed
//! outputs/tickets transfer together before any live metadata installation.
//! No Backend receiver, storage receiver or original admission port is stored.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");
const State = @import("state.zig").State;
const Merge = @import("state_merge.zig").Heap;
const Comparison = @import("run_comparison.zig").Cursor;
const table = @import("../lsm/table_file.zig");
const io = @import("storage_io.zig");
const Producer = @import("registered_run_producer.zig").Owner;
const Output = @import("registered_run_output.zig");
const cleanup = @import("output_cleanup.zig");
const slice: resources.Slice = .lsm_table_builder_working_set;

pub const Config = struct {
    first_id: u64,
    end_id: u64,
    output_level: u32 = 0,
    visibility_id: ?u64 = null,
    gc_requested: bool = false,
    oldest_tombstone_unix_ns: u64 = 0,
    target_bytes: usize = 4 << 20,
    max_file_bytes: usize = 64 << 20,
    max_entries: usize = std.math.maxInt(u32),
    partition_prefix_bytes: usize = 0,
    partition_key: ?*const fn ([]const u8, admission.Port) anyerror![]const u8 = null,
    encoder: table.StreamingEncoderOptions = .{},
};
/// Sole caller ownership, including the final-file path obligation. This is
/// construction output, never a durability receipt. The publication controller
/// retains unpublished deletion authority until an irrevocable live-root handoff
/// (as in Run.commitOutput) or a resolved publication outcome. Manifest/WAL debt
/// and accepted physical IO remain owned separately through completion/uncertainty.
pub const Completed = struct {
    id: u64,
    visibility_id: u64,
    level: u32 = 0,
    gc_requested: bool = false,
    oldest_tombstone_unix_ns: u64 = 0,
    rows: usize,
    tombstones: u32,
    output: *Output.Owner,
    ticket: *cleanup.Ticket,
};
const Node = struct {
    next: ?*Node = null,
    credit: resources.Reservation,
    grant_credit: ?resources.Reservation = null,
    id: u64,
    path: [std.fs.max_path_bytes]u8 = undefined,
    path_len: usize = 0,
    creation: ?cleanup.Creation = null,
    ticket: ?*cleanup.Ticket = null,
    grant: ?io.DurabilityResources = null,
    output: ?*Output.Owner = null,
    producer: ?*Producer = null,
    phase: enum { output, ticket, grant, producer, open, ready, finish, handoff, take_ticket, retire, done } = .output,
    rows: usize = 0,
    tombstones: u32 = 0,
    logical_bytes: usize = 0,
    first_namespace: ?[]const u8 = null,
    first_domain: ?[]const u8 = null,
};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    queue: *cleanup.Queue,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    config: Config,
    root: [std.fs.max_path_bytes]u8 = undefined,
    root_len: usize,
    heap: Merge,
    plan_index: usize = 0,
    input_entries: usize = 0,
    consumed: usize = 0,
    emitted: usize = 0,
    next_id: u64,
    head: ?*Node = null,
    tail: ?*Node = null,
    active: ?*Node = null,
    take_next: ?*Node = null,
    pending_node_credit: ?resources.Reservation = null,
    pending: ?Merge.Winner = null,
    pending_domain: ?[]const u8 = null,
    pending_bytes: usize = 0,
    partition: ?Comparison = null,
    stage: enum { plan, initialize, select, prepare, append, advance, finish, done } = .plan,
    closing: bool = false,
    handoff_owner: ?*anyopaque = null,
    complete: bool = false,

    pub fn start(a: std.mem.Allocator, manager: *resources.ResourceManager, queue: *cleanup.Queue, root: []const u8, states: []const *const State, config: Config, scope: admission.Port) !*Owner {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (queue.manager != manager or queue.allocator.ptr != a.ptr or queue.allocator.vtable != a.vtable) return error.ResourceAccountingMismatch;
        if (root.len == 0 or root.len >= std.fs.max_path_bytes) return error.NameTooLong;
        if (config.first_id >= config.end_id or config.visibility_id == 0 or config.max_entries == 0 or config.max_entries > std.math.maxInt(u32) or config.encoder.compression_stats != null) return error.InvalidArgument;
        const heap = try Merge.prepare(a, states);
        const arrays = (try Merge.memoryBound(states.len)) - @sizeOf(Merge);
        const bound = std.math.add(u64, @sizeOf(Owner), arrays) catch return error.ResourceBudgetExceeded;
        try queue.retainProducerWithAdmission(original);
        var queue_retained = true;
        defer if (queue_retained) queue.releaseProducer();
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(slice, bound, original);
        defer construction.retire();
        const self = try a.create(Owner);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .queue = queue, .config = config, .root_len = root.len, .heap = heap, .next_id = config.first_id };
        @memcpy(self.root[0..root.len], root);
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = retire }, destroy, original);
        registered = true;
        host = null;
        queue_retained = false;
        return self;
    }
    fn domain(self: *Owner, key: []const u8, original: admission.Port) ![]const u8 {
        if (self.config.partition_key) |extract| return extract(key, original);
        return key[0..@min(key.len, self.config.partition_prefix_bytes)];
    }
    fn entryBytes(row: table.Entry) !usize {
        var len = std.math.add(usize, 13, row.key.len) catch return error.ResourceBudgetExceeded;
        len = std.math.add(usize, len, row.value.len) catch return error.ResourceBudgetExceeded;
        return std.math.add(usize, len, if (row.namespace_name) |ns| ns.len else 0) catch error.ResourceBudgetExceeded;
    }
    fn createNode(self: *Owner, original: admission.Port) !void {
        if (self.next_id >= self.config.end_id) return error.FlushRunIdReservationExhausted;
        if (self.pending_node_credit == null) self.pending_node_credit = try self.manager.reserveWithoutReclaimWithAdmission(slice, @sizeOf(Node), original);
        const node = try self.allocator.create(Node);
        node.* = .{ .credit = self.pending_node_credit.?, .id = self.next_id };
        self.pending_node_credit = null;
        if (self.tail) |tail| tail.next = node else self.head = node;
        self.tail = node;
        self.active = node;
        self.next_id += 1;
        // A path failure retains the attached node; it is never replayed with
        // another ID or left outside registered cleanup ownership.
        self.stage = .prepare;
        const root = self.root[0..self.root_len];
        const path = std.fmt.bufPrint(&node.path, "{s}{s}runs/{d}.tbl", .{ root, if (root[root.len - 1] == '/') "" else "/", node.id }) catch return error.NameTooLong;
        node.path_len = path.len;
        node.creation = cleanup.Creation.init(self.queue, node.path[0..node.path_len]);
    }
    fn prepareNode(self: *Owner, storage: io.Storage, node: *Node, original: admission.Port) !void {
        while (node.phase != .ready) {
            try original.checkpoint();
            switch (node.phase) {
                .output => {
                    node.output = try Output.Owner.start(self.allocator, self.manager, original);
                    node.phase = .ticket;
                },
                .ticket => {
                    const creation = if (node.creation) |*value| value else return error.NameTooLong;
                    try creation.resumeWithAdmission(original);
                    node.ticket = try creation.take();
                    node.phase = .grant;
                },
                .grant => {
                    if (node.grant_credit == null) node.grant_credit = try self.manager.reserveWithoutReclaimWithAdmission(slice, try storage.appendResourceBytes(0), original);
                    node.grant = (try storage.tryPrepareDurabilityWithAdmission(original)) orelse return error.ResourceBudgetExceeded;
                    node.phase = .producer;
                },
                .producer => {
                    const estimate = if (self.config.target_bytes == 0) self.input_entries - self.consumed else @min(self.input_entries - self.consumed, @max(@as(usize, 1), self.config.target_bytes / 13));
                    node.producer = try Producer.start(self.allocator, self.manager, &node.grant, try storage.appendResourceBytes(0), &node.ticket, @max(1, estimate), self.config.max_file_bytes, self.config.encoder, original);
                    node.grant_credit.?.release();
                    node.grant_credit = null;
                    node.phase = .open;
                },
                .open => {
                    try (try node.producer.?.drivingWriter()).openWithAdmission(original);
                    node.phase = .ready;
                },
                else => return error.InvalidArgument,
            }
        }
    }
    fn finishNode(self: *Owner, node: *Node, original: admission.Port) !void {
        if (node.phase == .ready) node.phase = .finish;
        while (node.phase != .done) {
            try original.checkpoint();
            const producer = node.producer orelse return error.InvalidArgument;
            switch (node.phase) {
                .finish => {
                    try (try producer.drivingWriter()).finishWithAdmission(original);
                    node.phase = .handoff;
                },
                .handoff => {
                    try (try producer.drivingWriter()).takeResultInto(&node.output.?.output, original);
                    node.phase = .take_ticket;
                },
                .take_ticket => {
                    node.ticket = try producer.takeTicketWithAdmission(original);
                    node.phase = .retire;
                },
                .retire => {
                    try producer.writer.?.retireWithAdmission(original);
                    producer.requestRetirement();
                    node.producer = null;
                    node.first_namespace = null;
                    node.first_domain = null;
                    node.phase = .done;
                },
                else => return error.InvalidArgument,
            }
        }
        self.active = null;
        self.stage = .select;
    }
    /// The storage provider and immutable inputs are the same throughout this
    /// serialized continuation. Passed receivers are borrowed only for this call.
    pub fn driveWithAdmission(self: *Owner, storage: io.Storage, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        while (self.stage != .done) {
            try original.checkpoint();
            switch (self.stage) {
                .plan => {
                    while (self.plan_index < self.heap.states.len) {
                        try original.checkpoint();
                        self.input_entries = std.math.add(usize, self.input_entries, self.heap.states[self.plan_index].entryCount()) catch return error.ResourceBudgetExceeded;
                        self.plan_index += 1;
                    }
                    if (self.input_entries == 0) return error.EmptyRun;
                    self.stage = .initialize;
                },
                .initialize => {
                    try self.heap.initializeWithAdmission(original);
                    self.stage = .select;
                },
                .select => {
                    if (self.pending == null) self.pending = try self.heap.peekWithAdmission(original);
                    if (self.pending == null) {
                        if (self.active != null) {
                            self.stage = .finish;
                            continue;
                        }
                        if (self.consumed != self.input_entries) return error.InvalidTableFile;
                        self.heap.beginAbort(); // No input read after completed build.
                        self.take_next = self.head;
                        self.stage = .done;
                        continue;
                    }
                    const row = self.pending.?.entry;
                    if (self.pending_domain == null) self.pending_domain = try self.domain(row.key, original);
                    const bytes = try entryBytes(row);
                    self.pending_bytes = bytes;
                    if (self.active) |node| {
                        _ = std.math.add(usize, node.logical_bytes, bytes) catch return error.ResourceBudgetExceeded;
                        var boundary = node.rows >= self.config.max_entries;
                        if (self.config.target_bytes != 0 and node.rows != 0) boundary = boundary or node.logical_bytes >= self.config.target_bytes or bytes > self.config.target_bytes -| node.logical_bytes;
                        if (node.rows != 0 and (self.config.partition_prefix_bytes != 0 or self.config.partition_key != null)) {
                            if (self.partition == null) self.partition = Comparison.bound(node.first_namespace, node.first_domain.?, row.namespace_name, self.pending_domain.?);
                            const order = try self.partition.?.resumeWithAdmission(original);
                            self.partition = null;
                            boundary = boundary or order != .eq;
                        }
                        if (!boundary) boundary = !(try (try node.producer.?.drivingWriter()).canAppendEntry(row));
                        if (boundary) {
                            if (node.rows == 0) return error.TableFileTooLarge;
                            self.stage = .finish;
                            continue;
                        }
                    } else {
                        try self.createNode(original);
                        continue;
                    }
                    self.stage = .append;
                },
                .prepare => {
                    try self.prepareNode(storage, self.active.?, original);
                    self.stage = .select;
                },
                .append => {
                    const node = self.active.?;
                    const row = self.pending.?.entry;
                    try (try node.producer.?.drivingWriter()).appendEntryWithAdmission(row, original);
                    if (node.rows == 0) {
                        node.first_namespace = row.namespace_name;
                        node.first_domain = self.pending_domain;
                    }
                    node.rows += 1;
                    node.tombstones += @intFromBool(row.tombstone);
                    node.logical_bytes += self.pending_bytes;
                    self.emitted += 1;
                    self.stage = .advance;
                },
                .advance => {
                    self.consumed += try self.heap.advanceWithAdmission(self.pending.?.entry, original);
                    self.pending = null;
                    self.pending_domain = null;
                    self.stage = .select;
                },
                .finish => try self.finishNode(self.active.?, original),
                .done => unreachable,
            }
        }
    }
    pub const CompletedRange = struct { first_id: u64, end_id: u64, level: u32, visibility_id: u64, gc_requested: bool, oldest_tombstone_unix_ns: u64 };

    /// Exclusive handoff claim. All allocation/registration in the receiver
    /// precedes this fixed transfer; refusal leaves the builder unclaimed.
    pub fn claimCompletedWithAdmission(self: *Owner, receiver: *anyopaque, original: admission.Port) !CompletedRange {
        try original.checkLifetime();
        if (self.closing or self.stage != .done or self.handoff_owner != null or self.take_next != self.head) return error.InvalidArgument;
        self.handoff_owner = receiver;
        return .{ .first_id = self.config.first_id, .end_id = self.next_id, .level = self.config.output_level, .visibility_id = self.config.visibility_id orelse self.next_id - 1, .gc_requested = self.config.gc_requested, .oldest_tombstone_unix_ns = self.config.oldest_tombstone_unix_ns };
    }

    /// Returns sole output creator/ticket ownership, never a borrowed header.
    pub fn takeCompletedWithAdmission(self: *Owner, original: admission.Port) !?Completed {
        try original.checkLifetime();
        if (self.handoff_owner != null) return error.InvalidArgument;
        return self.takeCompletedFixed();
    }
    pub fn takeCompletedForOwnerWithAdmission(self: *Owner, receiver: *anyopaque, original: admission.Port) !?Completed {
        try original.checkLifetime();
        if (self.handoff_owner != receiver) return error.InvalidArgument;
        return self.takeCompletedFixed();
    }
    pub fn completedExhaustedForOwnerWithAdmission(self: *Owner, receiver: *anyopaque, original: admission.Port) !bool {
        try original.checkLifetime();
        if (self.closing or self.stage != .done or self.handoff_owner != receiver) return error.InvalidArgument;
        return self.take_next == null;
    }
    fn takeCompletedFixed(self: *Owner) !?Completed {
        if (self.closing or self.stage != .done) return error.InvalidArgument;
        const node = self.take_next orelse return null;
        const result: Completed = .{ .id = node.id, .visibility_id = self.config.visibility_id orelse self.next_id - 1, .level = self.config.output_level, .gc_requested = self.config.gc_requested, .oldest_tombstone_unix_ns = self.config.oldest_tombstone_unix_ns, .rows = node.rows, .tombstones = node.tombstones, .output = node.output.?, .ticket = node.ticket.? };
        node.output = null;
        node.ticket = null;
        self.take_next = node.next;
        return result;
    }
    /// Final caller access. Stop all input readers before publishing cleanup.
    pub fn requestRetirement(self: *Owner) void {
        if (self.closing) return;
        self.closing = true;
        self.heap.beginAbort();
        self.pending = null;
        self.pending_domain = null;
        self.partition = null;
        if (self.active) |node| {
            node.first_namespace = null;
            node.first_domain = null;
            if (node.producer) |producer| {
                producer.requestRetirement();
                node.producer = null;
            }
        }
        self.registration.publish();
    }
    fn retire(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.head) |node| {
            if (node.producer) |producer| {
                producer.requestRetirement();
                node.producer = null;
            }
            if (node.creation) |*creation| try creation.retireWithAdmission(original);
            node.creation = null;
            if (node.grant) |*grant| {
                if (original) |port| try grant.retireWithAdmission(port) else grant.deinit();
                node.grant = null;
            }
            if (node.grant_credit) |*credit| credit.release();
            node.grant_credit = null;
            if (node.ticket) |ticket| {
                if (original) |port| try ticket.abandonWithAdmission(port) else ticket.abandon();
                node.ticket = null;
            }
            if (node.output) |output| output.requestRetirement();
            node.output = null;
            self.head = node.next;
            const a = self.allocator;
            var credit = node.credit;
            a.destroy(node);
            credit.release();
            return false;
        }
        if (self.pending_node_credit) |*credit| credit.release();
        self.pending_node_credit = null;
        if (!try self.heap.retireStep(original)) return false;
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Owner) <= 16 * 1024 and @sizeOf(Node) <= 16 * 1024);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.head == null);
        const a = self.allocator;
        const host = self.host;
        const queue = self.queue;
        a.destroy(self);
        queue.releaseProducer();
        host.release();
    }
};
