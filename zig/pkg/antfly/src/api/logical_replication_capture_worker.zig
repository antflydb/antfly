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

const std = @import("std");
const local = @import("antfly_local_sources");
const installation = @import("../system_catalog/replication/installation.zig");
const Context = local.api_operation.RequestContext;
pub const Capture = local.system_catalog_logical_replication_capture;
pub const Proofs = @import("logical_replication_publisher_attestation.zig");
pub const Receipt = union(enum) { observation: Proofs.ObservationReceipt, guard: Proofs.GuardReceipt };

pub const suffix = "/publication-capture";
pub const budget_header = "x-antfly-publication-capture-budget-ms";
pub const max_budget_ms = 5_000;
pub const max_request_bytes = 32 * 1024;
pub const max_response_bytes = 32 * 1024;

pub fn ownerGroup(request: Capture.Request) !u64 {
    return switch (request) {
        .observe => |value| blk: {
            try value.validate();
            break :blk value.owner_group_id;
        },
        .apply => |value| blk: {
            try value.validate();
            break :blk value.binding.owner_group_id;
        },
    };
}

pub const Capacity = local.system_catalog_logical_replication_capacity;
pub const outbox_suffix = "/publication-outbox";
pub const terminal_suffix = "/publication-terminal";
pub const collection_suffix = "/publication-collection";
pub const outbox_page_suffix = "/publication-outbox-page";
pub const OutboxPage = local.storage_db_logical_replication_outbox_contract;
pub const OutboxPageRequest = struct {
    access: Publisher.OutboxRead,
    after: u64,
    limit: u16,
    max_bytes: usize,

    pub fn page(self: @This()) OutboxPage.PageRequest {
        return .{ .reference = self.access.reference, .after = self.after, .limit = self.limit, .max_bytes = self.max_bytes };
    }
};
pub const max_collection_request_bytes = 32 * 1024;
pub const Publisher = local.system_catalog_logical_replication_publisher;
const journal = @import("../system_catalog/replication/decision_journal.zig");
pub const CollectionCursor = journal.CollectionCursor;
pub const max_outbox_request_bytes = local.system_catalog_logical_replication_publisher.max_roster_bytes + 32 * 1024;
pub const capacity_suffix = "/publication-capacity";
pub const max_capacity_request_bytes = 16 * 1024 * 1024;

pub const ReceiptKind = enum {
    capacity,
    outbox,
    terminal,

    pub fn Result(comptime self: @This()) type {
        return switch (self) {
            .capacity => Proofs.CapacityReceipt,
            .outbox => ?Proofs.OutboxReceipt,
            .terminal => ?Proofs.TerminalResult,
        };
    }

    pub fn suffix(self: @This()) []const u8 {
        return switch (self) {
            .capacity => capacity_suffix,
            .outbox => outbox_suffix,
            .terminal => terminal_suffix,
        };
    }
};

pub const Driver = struct {
    ptr: *anyopaque,
    read_page: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, OutboxPageRequest, Context) anyerror![]u8 = null,
    collect: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, Publisher.GcPermission, Context) anyerror!Proofs.CollectionReceipt = null,
    observe_outbox: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, Capacity.Observation, Context) anyerror!?Proofs.OutboxReceipt = null,
    observe_terminal: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, Capacity.Observation, Context) anyerror!?Proofs.TerminalResult = null,
    observe_capacity: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, Capacity.Request, Context) anyerror!Proofs.CapacityReceipt = null,
    execute: *const fn (*anyopaque, std.mem.Allocator, []const u8, Capture.Request, Context) anyerror!Receipt,
};

/// Each tick advances at most one journal phase or physical control. Cursor
/// movement before execution lets unavailable operations share the lane.
pub fn step(backing: std.mem.Allocator, source: anytype, driver: Driver, after: *?installation.Address, context: Context) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try context.ensureActive();
    const bytes = try source.systemCatalog(a, context, .{ .sql_publication_capture_work = .{ .after = after.* } });
    if (bytes.len > installation.max_work_bytes) return error.CatalogCommandTooLarge;
    const found = try std.json.parseFromSliceLeaky(?installation.Work, a, bytes, .{ .allocate = .alloc_always });
    after.* = if (found) |work| work.address else null;
    const work = found orelse return;
    var incarnation: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&incarnation, &work.incarnation);
    const action: installation.Advance = switch (work.action) {
        .observe => |tasks| observed: {
            const receipts = try a.alloc(Proofs.ObservationReceipt, tasks.len);
            for (tasks, receipts) |task, *receipt| {
                try context.ensureActive();
                const signed = try driver.execute(driver.ptr, a, task.table, .{ .observe = task.request }, context);
                if (signed != .observation) return error.InvalidLogicalCaptureControl;
                try signed.observation.validate(task.request, a);
                receipt.* = signed.observation;
            }
            break :observed .{ .bind = receipts };
        },
        .apply => |task| applied: {
            const signed = try driver.execute(driver.ptr, a, task.table, .{ .apply = task.request }, context);
            if (signed != .guard) return error.InvalidLogicalCaptureControl;
            try signed.guard.validate(task.request, a);
            break :applied .{ .receipt = signed.guard };
        },
        .publish => .publish,
    };
    try context.ensureActive();
    _ = try source.systemCatalog(a, context, .{ .sql_publication_advance = .{
        .address = work.address,
        .incarnation = incarnation,
        .expected_revision = work.revision,
        .action = action,
    } });
}

/// Advance one durable cleanup action. Move the cursor before doing I/O so a
/// blocked owner cannot starve the rest of the directory.
pub fn stepCollection(backing: std.mem.Allocator, source: anytype, driver: Driver, after: *?CollectionCursor, context: Context) !bool {
    const collect = driver.collect orelse return false;
    var arena: std.heap.ArenaAllocator = .init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    try context.ensureActive();
    const bytes = try source.systemCatalog(a, context, .{ .sql_publication_capture_work = .{ .collection = .{ .next = after.* } } });
    const decoded = try std.json.parseFromSliceLeaky(?journal.CollectionWork, a, bytes, .{});
    const work = decoded orelse {
        after.* = null;
        return false;
    };
    after.* = work.cursor;
    const receipt = try collect(driver.ptr, a, work.permission.debt.table_name, work.permission, context);
    try receipt.validate(a);
    if (!std.mem.eql(u8, &try work.permission.digest(a), &try receipt.permission.digest(a))) return error.LogicalCaptureControlChanged;
    try context.ensureActive();
    _ = try source.systemCatalog(a, context, .{ .sql_publication_collection_ack = receipt });
    return true;
}
