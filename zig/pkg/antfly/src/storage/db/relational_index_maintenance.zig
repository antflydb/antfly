// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
//! Maintenance is a normal replicated transaction intent, not a lookup side
//! effect. Replicas share desired maintenance but keep independent local build
//! progress; comparing the leader's local progress in Raft apply is incorrect.
const std = @import("std");
const contract = @import("relational_index_maintenance_contract.zig");
const jobs = @import("relational_index_jobs.zig");
const plans = @import("relational_index_plan.zig");
const transactions = @import("../transactions.zig");
const docstore = @import("../docstore.zig");

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    intent: transactions.WriteIntent,
    predicate: transactions.VersionPredicate,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Caller pins the active plan/schema and holds apply-exclusive. Returned
/// slices belong to its preparation arena until normal transaction admission.
pub fn prepareCommand(parent_alloc: std.mem.Allocator, txn: *docstore.DocStore.Txn, plan: plans.View, table_id: u64, command: contract.Command) !Prepared {
    var arena = std.heap.ArenaAllocator.init(parent_alloc);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    try command.validate();
    if (table_id != command.table_id or plan.schemaView().version() != command.schema_version) return error.PreparedGenerationChanged;
    const index = for (plan.boundIndexes()) |index| {
        if (std.mem.eql(u8, index.name, command.index_name)) break index;
    } else return error.PreparedGenerationChanged;
    if (index.generation != command.generation or index.slot != command.slot or
        !std.mem.eql(u8, &index.tuple.fingerprint, &command.comparison)) return error.PreparedGenerationChanged;
    const range = try @import("range_state.zig").decodeRangeAlloc(alloc, (txn.get(@import("range_state.zig").range_key) catch |err| switch (err) {
        error.NotFound => &([_]u8{0} ** 8),
        else => return err,
    }));
    defer alloc.free(range.start);
    defer alloc.free(range.end);
    if (!std.mem.eql(u8, range.start, command.routing_key)) return error.PreparedGenerationChanged;
    const key = try alloc.dupe(u8, &contract.controlKey(index.id()));
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (!std.mem.eql(u8, &(try jobs.ownership(txn)), &command.owner)) return error.PreparedGenerationChanged;
    var current = try contract.readControl(txn, index.id());
    const identity = command.fingerprint();
    const already_applied = std.mem.eql(u8, &current.last_request, &identity);
    const expected = if (raw) |bytes| try alloc.dupe(u8, bytes) else null;
    if (!already_applied) {
        if (current.epoch != command.expected_maintenance_epoch) return error.PreparedGenerationChanged;
        current.epoch = std.math.add(u64, current.epoch, 1) catch return error.InvalidBatchRequest;
        current.last_request = identity;
    }
    // Finish every arena allocation before moving its state into Prepared.
    // Encoding after copying arena can allocate a block absent from that copy.
    const value = try current.encode(alloc);
    return .{
        .arena = arena,
        .intent = .{ .key = key, .value = value },
        .predicate = .{ .key = key, .comparison = .exact_value, .expected_value = expected },
    };
}
