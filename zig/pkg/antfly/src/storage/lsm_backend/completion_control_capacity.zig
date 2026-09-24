// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Lifetime sizing for separately retained transaction control owners. Inputs
//! come from the validated control compiler and immutable native codec, never
//! an untrusted envelope's claimed size. A certificate grants no log authority
//! and owns no backing. All lifetime costs remain charged through final ACK;
//! checkpointing or overwriting a row does not refund them.
const std = @import("std");
const capacity = @import("completion_capacity.zig");
const domains = @import("completion_allocator.zig");
const state = @import("state.zig");
const record = @import("completion_control_record.zig");
const entry = @import("completion_entry.zig");
const internal_keys = @import("../internal_keys.zig");

pub const max_owners = capacity.control_outputs;
const max_namespace_bytes = "docs".len;

fn add(a: u64, b: u64) !u64 {
    return std.math.add(u64, a, b) catch error.UnsupportedCompletionProfile;
}
fn mul(a: u64, b: u64) !u64 {
    return std.math.mul(u64, a, b) catch error.UnsupportedCompletionProfile;
}
fn size(value: u64) !usize {
    return std.math.cast(usize, value) orelse error.UnsupportedCompletionProfile;
}

/// Cycle-free projection of transactions.CompletionControlBudget. It includes
/// all public begin/decision/unique-ACK writes, but no native ownership records.
pub const NumericBudget = struct {
    mutations: u64,
    operations: u64,
    payload_bytes: u64,
    max_mutation_payload_bytes: u64,
    max_key_bytes: u64,
    max_record_payload_bytes: u64,
    wal_bytes: u64,

    pub fn fromMeasured(measured: anytype) NumericBudget {
        var result: NumericBudget = undefined;
        inline for (std.meta.fields(NumericBudget)) |field| @field(result, field.name) = @field(measured, field.name);
        return result;
    }

    fn validate(self: NumericBudget) !void {
        if (self.mutations == 0 or self.operations < self.mutations or self.max_key_bytes == 0 or
            self.max_key_bytes > self.max_record_payload_bytes or self.max_record_payload_bytes > self.max_mutation_payload_bytes or
            self.max_mutation_payload_bytes > self.payload_bytes) return error.UnsupportedCompletionProfile;
        const framed = try add(self.payload_bytes, try add(try mul(self.mutations, 20), try mul(self.operations, 16 + max_namespace_bytes)));
        if (self.wal_bytes < framed) return error.UnsupportedCompletionProfile;
    }
};

/// Codec-defined physical rows, counting tombstones as zero-value writes.
/// Shapes describe all writes over the lifetime, not just the latest value.
pub const NativeRowShape = struct {
    key_bytes: usize,
    value_bytes: usize,
    max_writes: u64,
};

/// The immutable declaration is inserted at BEGIN and deleted at terminal ACK.
/// The latest receipt remains durable; its later history cleanup is new work.
pub fn ownershipRows(transitions: u64) [3]NativeRowShape {
    return .{
        .{ .key_bytes = record.owner_key_bytes, .value_bytes = record.encoded_bytes, .max_writes = 1 },
        .{ .key_bytes = record.owner_key_bytes, .value_bytes = 0, .max_writes = 1 },
        .{ .key_bytes = record.progress_key_bytes, .value_bytes = record.progress_bytes, .max_writes = transitions },
    };
}

pub const OwnerInput = struct {
    budget: NumericBudget,
    /// Native owner/latest-receipt shapes. Progress and applied-marker rows are
    /// always added below from their actual native formats, once per transition.
    rows: []const NativeRowShape,
};

pub const Base = struct {
    /// Persisted baseline plus all foreground/document obligations, including
    /// spent cells that cannot be refunded before maintenance.
    cost: capacity.Cost,
    mutable_growth: capacity.Cost,
    baseline_frontier_bytes: u64,
    max_mutable_entries: usize,
    current_runs: usize,
    future_document_outputs: usize,
    append: capacity.AppendCounters,
};

pub const Limits = struct {
    format: capacity.Limits = .{},
    max_record_bytes: usize,
    /// Must be backed by actual path pins, cursors and publication metadata.
    /// Current document-only installations have 68; four controls require 72.
    max_inputs: usize,
    max_path_bytes: usize,
    single_drain_metadata_bytes: usize,
    max_retained_wal_bytes: u64,
    replay_workspace_bytes: usize,
    compiler_workspace_bytes: usize,
};

pub const OwnerCertificate = struct {
    growth: capacity.Cost = .{},
    append: capacity.AppendCounters = .{},
    payload_bytes: u64 = 0,
    publication_bytes: usize = 0,
    publication_domain_bytes: usize = 0,
    checkpoint_metadata_bytes: usize = 0,
};

pub const NativeCounterReserve = struct {
    manifest_steps: u64 = 0,
    run_ids: u64 = 0,
    wal_segments: u64 = 0,
};

pub const Certificate = struct {
    owners: [max_owners]OwnerCertificate = @splat(.{}),
    owner_count: usize,
    growth: capacity.Cost,
    total_cost: capacity.Cost,
    append: capacity.AppendCounters,
    publication_bytes: usize,
    publication_domain_bytes: usize,
    max_mutable_entries: usize,
    required_run_frontier: usize,
    final_checkpoint_outputs: usize,
    format: capacity.Certificate,
    additional_counters: NativeCounterReserve,
    current_frontier_bytes: u64 = 0,
    replay_workspace_bytes: usize = 0,
    replay_path_allocations: usize = 0,
    operation_workspace_bytes: usize = 0,
    maintenance_workspace_bytes: usize = 0,

    /// Checks the complete document/foreground/control append reserve and adds
    /// each control's terminal checkpoint/rotation successors to the caller's
    /// existing native counter reservation. Never saturate/reset these values.
    pub fn requireHeadroom(self: Certificate, stats: anytype, manifest_sequence: u64, next_run_id: u64, wal_segment: u64, existing: NativeCounterReserve) !void {
        try self.append.requireHeadroom(stats, .{});
        _ = try add(manifest_sequence, try add(existing.manifest_steps, self.additional_counters.manifest_steps));
        _ = try add(next_run_id, try add(existing.run_ids, self.additional_counters.run_ids));
        _ = try add(wal_segment, try add(existing.wal_segments, self.additional_counters.wal_segments));
    }
};

fn publicCost(budget: NumericBudget) !capacity.Cost {
    const key = try add(budget.max_key_bytes, max_namespace_bytes);
    const encoded = try add(budget.payload_bytes, try mul(budget.operations, 13 + max_namespace_bytes));
    return .{
        .records = budget.operations,
        .encoded_bytes = encoded,
        .metadata_bytes = try mul(budget.operations, try add(capacity.record_metadata_bytes, try mul(2, key))),
        .block_weight = @min(encoded, try mul(budget.operations, capacity.block_bytes)),
        .max_key_bytes = key,
        .max_record_bytes = try add(budget.max_record_payload_bytes, 13 + max_namespace_bytes),
    };
}

fn addRows(out: *OwnerCertificate, shape: NativeRowShape) !void {
    if (shape.max_writes == 0) return;
    if (shape.key_bytes == 0) return error.UnsupportedCompletionProfile;
    const payload = try mul(try add(max_namespace_bytes, try add(shape.key_bytes, shape.value_bytes)), shape.max_writes);
    const cost = try (try capacity.Cost.record(max_namespace_bytes, shape.key_bytes, shape.value_bytes)).repeated(shape.max_writes);
    out.growth = try out.growth.plus(cost);
    out.payload_bytes = try add(out.payload_bytes, payload);
    // Row headers only: public budget already includes every WAL frame header.
    out.append = try out.append.plus(.{ .bytes = try add(payload, try mul(16, shape.max_writes)), .entries = shape.max_writes });
}

pub fn certifyCohort(base: Base, inputs: []const OwnerInput, limits: Limits) !Certificate {
    if (inputs.len == 0 or inputs.len > max_owners or limits.max_path_bytes == 0 or limits.single_drain_metadata_bytes > limits.format.metadata_bytes)
        return error.UnsupportedCompletionProfile;
    var result = Certificate{
        .owner_count = inputs.len,
        .growth = .{},
        .total_cost = base.cost,
        .append = base.append,
        .publication_bytes = 0,
        .publication_domain_bytes = 0,
        .max_mutable_entries = base.max_mutable_entries,
        .required_run_frontier = 0,
        .final_checkpoint_outputs = inputs.len,
        .format = undefined,
        .additional_counters = .{ .manifest_steps = inputs.len, .run_ids = inputs.len },
    };
    for (inputs, 0..) |input, i| {
        try input.budget.validate();
        var owner = OwnerCertificate{
            .growth = try publicCost(input.budget),
            .append = .{ .bytes = input.budget.wal_bytes, .entries = input.budget.operations, .records = input.budget.mutations },
            .payload_bytes = try add(input.budget.payload_bytes, try mul(max_namespace_bytes, input.budget.operations)),
        };
        for (input.rows) |row| try addRows(&owner, row);
        try addRows(&owner, .{ .key_bytes = entry.group_progress_key.len, .value_bytes = 112, .max_writes = input.budget.mutations });
        try addRows(&owner, .{ .key_bytes = internal_keys.raft_document_applied_entry_key.len, .value_bytes = 16, .max_writes = input.budget.mutations });
        if (owner.growth.max_record_bytes > limits.max_record_bytes) return error.UnsupportedCompletionProfile;
        result.owners[i] = owner;
        result.growth = try result.growth.plus(owner.growth);
        result.append = try result.append.plus(owner.append);
        result.additional_counters.wal_segments = try add(result.additional_counters.wal_segments, owner.append.records);
    }
    // Distinct keys cannot exceed cumulative inserted/tombstoned records. This
    // includes all four owners even if each publication has a retained reader.
    result.max_mutable_entries = try size(try add(base.max_mutable_entries, result.growth.records));
    result.total_cost = try base.cost.plus(result.growth);
    var format_limits = limits.format;
    format_limits.additional_runs = try add(base.future_document_outputs, inputs.len);
    result.format = try capacity.certify(result.total_cost, format_limits);
    result.required_run_frontier = try size(try add(@max(base.current_runs, result.format.outputs), format_limits.additional_runs));
    if (result.required_run_frontier > limits.max_inputs) return error.UnsupportedCompletionProfile;
    // Every final checkpoint can see the complete cumulative mutable growth.
    // Existing immutable baseline is excluded from a single mutable drain.
    try capacity.certifySingleDrain(try base.mutable_growth.plus(result.growth), limits.single_drain_metadata_bytes);
    const mutable = try base.mutable_growth.plus(result.growth);
    const added = try capacity.certify(mutable, format_limits);
    result.current_frontier_bytes = try add(base.baseline_frontier_bytes, added.frontier_bytes);
    if (result.current_frontier_bytes > limits.format.frontier_bytes or result.append.bytes > limits.max_retained_wal_bytes)
        return error.UnsupportedCompletionProfile;
    const footprint = domains.RecyclingScratch.allocationFootprint;
    const replay_tree = try state.ActiveMemTable.uniqueReplayAllocationBound(try size(try add(result.append.entries, 1)), try size(result.append.bytes));
    const replay_pending = try footprint(try size(result.append.bytes), 1);
    // Same parser/retention passes as the pooled restore proof, now including
    // every control append, even tiny segments and repeatedly overwritten keys.
    result.replay_path_allocations = try size(try add(12, try mul(3, try add(result.append.records, 1))));
    const replay_paths = try std.math.mul(usize, result.replay_path_allocations, try footprint(limits.max_path_bytes, 1));
    result.replay_workspace_bytes = try std.math.add(usize, replay_tree, try std.math.add(usize, replay_pending, replay_paths));
    if (result.replay_workspace_bytes > limits.replay_workspace_bytes) return error.UnsupportedCompletionProfile;
    const maintenance = @import("completion_maintenance.zig");
    const writer_limits: maintenance.Limits = .{
        .max_inputs = limits.max_inputs,
        .max_metadata_bytes = try size(limits.format.metadata_bytes),
        .max_output_metadata_bytes = limits.single_drain_metadata_bytes,
        .max_record_bytes = limits.max_record_bytes,
        .max_output_file_bytes = try size(limits.format.file_bytes),
    };
    const workspace = try maintenance.workspaceRequirement(result.total_cost, @max(result.format.frontier_bytes, result.current_frontier_bytes), result.format.outputs, writer_limits);
    result.maintenance_workspace_bytes = workspace.total;
    const completion = @import("completion_runtime.zig");
    const operation = try completion.operationWorkspaceRequirement(mutable, writer_limits);
    // Existing operation certificate includes document/foreground reset paths;
    // control records add at most one extra segment each before that checkpoint.
    const extra_paths = try std.math.mul(usize, try size(result.additional_counters.wal_segments), try footprint(limits.max_path_bytes, 1));
    result.operation_workspace_bytes = try std.math.add(usize, operation.total, extra_paths);
    if (result.maintenance_workspace_bytes > limits.compiler_workspace_bytes or result.operation_workspace_bytes > limits.compiler_workspace_bytes)
        return error.UnsupportedCompletionProfile;
    const boundary = try size(result.total_cost.max_key_bytes);
    const checkpoint_metadata = try std.math.add(usize, try @import("run_store.zig").Store.singleInsertAllocationBound(result.required_run_frontier, boundary, limits.max_path_bytes), try @import("run_directory.zig").Directory.singleInsertAllocationBound(result.required_run_frontier, boundary, limits.max_path_bytes));
    for (result.owners[0..inputs.len]) |*owner| {
        owner.checkpoint_metadata_bytes = checkpoint_metadata;
        owner.publication_bytes = try std.math.add(usize, try state.ActiveMemTable.cumulativePublicationAllocationBound(result.max_mutable_entries, try size(owner.growth.records), try size(owner.payload_bytes)), checkpoint_metadata);
        owner.publication_domain_bytes = try domains.PublicationReservation.backingFootprint(owner.publication_bytes);
        result.publication_bytes = try std.math.add(usize, result.publication_bytes, owner.publication_bytes);
        result.publication_domain_bytes = try std.math.add(usize, result.publication_domain_bytes, owner.publication_domain_bytes);
    }
    return result;
}

/// Rebuilds the complete native control reserve from canonical BEGINs retained
/// in local guards. No metadata service or transaction-manager scan supplies
/// sizing inputs during restart; the same aggregate certificate used at BEGIN
/// must be checked before a restored owner can become runnable.
pub fn certifyRestored(base: Base, restored: *const @import("completion_control_guard.zig").Set, limits: Limits) !Certificate {
    if (restored.count == 0 or restored.count > max_owners) return error.UnsupportedCompletionProfile;
    var rows: [max_owners][3]NativeRowShape = undefined;
    var inputs: [max_owners]OwnerInput = undefined;
    var count: usize = 0;
    for (restored.owners) |owner| if (owner) |held| {
        if (count >= max_owners) return error.UnsupportedCompletionProfile;
        const budget = NumericBudget.fromMeasured(held.declaration.budget);
        rows[count] = ownershipRows(budget.mutations);
        inputs[count] = .{ .budget = budget, .rows = &rows[count] };
        count += 1;
    };
    if (count != restored.count) return error.InvalidCompletionSlot;
    return certifyCohort(base, inputs[0..count], limits);
}

fn fixtureBudget(transitions: u64) !NumericBudget {
    const operations = try mul(2, transitions);
    const payload = try mul(operations, 32 + 64);
    return .{
        .mutations = transitions,
        .operations = operations,
        .payload_bytes = payload,
        .max_mutation_payload_bytes = 2 * (32 + 64),
        .max_key_bytes = 32,
        .max_record_payload_bytes = 32 + 64,
        .wal_bytes = try add(payload, try add(try mul(20, transitions), try mul(20, operations))),
    };
}

fn fixtureBase() !Base {
    const mutable = try (try capacity.Cost.record(4, 32, 64)).repeated(2048 + 4 * 260);
    return .{
        .cost = try mutable.plus(try (try capacity.Cost.record(4, 32, 64)).repeated(10000)),
        .mutable_growth = mutable,
        .baseline_frontier_bytes = 4 * 1024 * 1024,
        .max_mutable_entries = 2048 + 4 * 260,
        .current_runs = 64,
        .future_document_outputs = 4,
        .append = try (try (try @import("completion_runtime.zig").prepare_append_budget.plus(@import("completion_runtime.zig").outcome_append_budget)).repeated(4)).plus(@import("completion_runtime.zig").foreground_append_budget),
    };
}

fn fixtureLimits() Limits {
    return .{ .format = .{ .metadata_bytes = 2 * 1024 * 1024 }, .max_record_bytes = 256 * 1024, .max_inputs = 72, .max_path_bytes = 512 + 32, .single_drain_metadata_bytes = 2 * 1024 * 1024, .max_retained_wal_bytes = 8 * 1024 * 1024, .replay_workspace_bytes = 32 * 1024 * 1024, .compiler_workspace_bytes = 32 * 1024 * 1024 };
}

test "workload admission completion control capacity combines document and four lifetime obligations" {
    const budget = try fixtureBudget(8);
    const rows = ownershipRows(budget.mutations);
    const owners = [_]OwnerInput{.{ .budget = budget, .rows = &rows }} ** 4;
    const base = try fixtureBase();
    const limits = fixtureLimits();
    const proof = try certifyCohort(base, &owners, limits);
    try std.testing.expectEqual(@as(usize, 72), proof.required_run_frontier);
    try std.testing.expectEqual(@as(usize, 4), proof.final_checkpoint_outputs);
    try std.testing.expectEqual(@as(u64, 32), proof.additional_counters.wal_segments);
    try std.testing.expectEqual(@as(u64, 4), proof.additional_counters.manifest_steps);
    try std.testing.expectEqual(@as(u64, 4), proof.additional_counters.run_ids);
    try std.testing.expectEqual(@as(u64, 4 * (16 + 3 * 8 + 2)), proof.growth.records);
    try std.testing.expectEqual(base.append.records + 32, proof.append.records);
    try std.testing.expect(proof.publication_domain_bytes > proof.publication_bytes);
    try std.testing.expect(proof.owners[0].checkpoint_metadata_bytes != 0);
    var insufficient = limits;
    insufficient.max_inputs = 68;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &owners, insufficient));
    insufficient = limits;
    insufficient.max_record_bytes = max_namespace_bytes + record.owner_key_bytes + record.encoded_bytes + 12;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &owners, insufficient));
    insufficient = limits;
    insufficient.single_drain_metadata_bytes = try size(proof.growth.metadata_bytes + capacity.fixed_file_bytes);
    // The controls fit by themselves, but cannot omit foreground/document rows.
    try capacity.certifySingleDrain(proof.growth, insufficient.single_drain_metadata_bytes);
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &owners, insufficient));
    insufficient = limits;
    insufficient.format.outputs = 1;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &owners, insufficient));
    insufficient = limits;
    insufficient.format.frontier_bytes = proof.format.frontier_bytes - 1;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &owners, insufficient));
    // A supported workload fits today's actual 1MiB metadata /68-input shape;
    // no increase to production limits is implied by the larger boundary case.
    var ordinary_limits = limits;
    ordinary_limits.max_inputs = 68;
    ordinary_limits.format.metadata_bytes = 1024 * 1024;
    ordinary_limits.single_drain_metadata_bytes = 1024 * 1024;
    var ordinary_base = base;
    ordinary_base.current_runs = 2;
    ordinary_base.mutable_growth = try (try capacity.Cost.record(4, 32, 64)).repeated(128);
    ordinary_base.max_mutable_entries = 128;
    const ordinary = try certifyCohort(ordinary_base, &owners, ordinary_limits);
    try std.testing.expect(ordinary.required_run_frontier <= 68);
    try std.testing.expect(ordinary.format.frontier_bytes <= 16 * 1024 * 1024);
    insufficient = limits;
    insufficient.max_retained_wal_bytes = proof.append.bytes - 1;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &owners, insufficient));
    insufficient = limits;
    insufficient.replay_workspace_bytes = proof.replay_workspace_bytes - 1;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &owners, insufficient));
    insufficient = limits;
    insufficient.compiler_workspace_bytes = @max(proof.operation_workspace_bytes, proof.maintenance_workspace_bytes) - 1;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &owners, insufficient));
    var crowded_base = base;
    crowded_base.baseline_frontier_bytes += limits.format.frontier_bytes - proof.current_frontier_bytes + 1;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(crowded_base, &owners, limits));
    const one = try certifyCohort(base, owners[0..1], limits);
    try std.testing.expect(proof.publication_bytes >= one.publication_bytes * 4);
    try std.testing.expect(proof.append.bytes > one.append.bytes);
    try std.testing.expect(proof.total_cost.metadata_bytes > one.total_cost.metadata_bytes);
    const five = [_]OwnerInput{owners[0]} ** 5;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(base, &five, limits));
}

test "workload admission completion control capacity protects append and checkpoint counter successors" {
    const budget = try fixtureBudget(8);
    const rows = ownershipRows(budget.mutations);
    const owners = [_]OwnerInput{.{ .budget = budget, .rows = &rows }} ** 4;
    const proof = try certifyCohort(try fixtureBase(), &owners, fixtureLimits());
    const max = std.math.maxInt(u64);
    const existing: NativeCounterReserve = .{ .manifest_steps = 6, .run_ids = 69, .wal_segments = 137 };
    const stats = .{ .wal_append_bytes = max - proof.append.bytes, .wal_append_entries = max - proof.append.entries, .wal_append_records = max - proof.append.records };
    const sequence = max - existing.manifest_steps - proof.additional_counters.manifest_steps;
    const run = max - existing.run_ids - proof.additional_counters.run_ids;
    const segment = max - existing.wal_segments - proof.additional_counters.wal_segments;
    try proof.requireHeadroom(stats, sequence, run, segment, existing);
    try std.testing.expectError(error.UnsupportedCompletionProfile, proof.requireHeadroom(stats, sequence + 1, run, segment, existing));
    try std.testing.expectError(error.UnsupportedCompletionProfile, proof.requireHeadroom(stats, sequence, run + 1, segment, existing));
    try std.testing.expectError(error.UnsupportedCompletionProfile, proof.requireHeadroom(stats, sequence, run, segment + 1, existing));
    inline for (.{ "wal_append_bytes", "wal_append_entries", "wal_append_records" }) |field| {
        var overflow = stats;
        @field(overflow, field) += 1;
        try std.testing.expectError(error.UnsupportedCompletionProfile, proof.requireHeadroom(overflow, sequence, run, segment, existing));
    }
    var invalid = budget;
    invalid.wal_bytes -= 1;
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(try fixtureBase(), &.{.{ .budget = invalid, .rows = &rows }}, fixtureLimits()));
    const oversized = [_]NativeRowShape{.{ .key_bytes = 1, .value_bytes = std.math.maxInt(usize), .max_writes = 1 }};
    try std.testing.expectError(error.UnsupportedCompletionProfile, certifyCohort(try fixtureBase(), &.{.{ .budget = budget, .rows = &oversized }}, fixtureLimits()));
}

test "workload admission completion control capacity covers interleaved publication and every retained reader" {
    const alloc = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    const transitions = 8;
    const budget = try fixtureBudget(transitions);
    const rows = ownershipRows(transitions);
    const inputs = [_]OwnerInput{.{ .budget = budget, .rows = &rows }} ** 4;
    const proof = try certifyCohort(try fixtureBase(), &inputs, fixtureLimits());
    var backing = std.testing.FailingAllocator.init(alloc, .{});
    var domains_owned: [4]?*domains.RecyclingScratch = @splat(null);
    var reservations: [4]?*domains.PublicationReservation = @splat(null);
    defer for (0..4) |i| {
        if (reservations[i]) |reservation| reservation.finish();
        if (domains_owned[i]) |domain| domain.retire();
    };
    for (0..4) |i| {
        domains_owned[i] = try domains.RecyclingScratch.create(backing.allocator(), &manager, proof.owners[i].publication_domain_bytes);
        reservations[i] = try domains.PublicationReservation.create(domains_owned[i].?, proof.owners[i].publication_bytes);
    }
    var live: state.ActiveMemTable = .{};
    defer live.deinit(alloc);
    var readers: [4 * transitions]?state.State = @splat(null);
    defer for (&readers) |*reader| if (reader.*) |*held| held.deinit(alloc);
    // A real document/foreground frontier is retained while controls alternate
    // publication allocators. Reader snapshots keep every old generation alive.
    for (0..proof.max_mutable_entries - @as(usize, @intCast(proof.growth.records))) |i| {
        const key = try std.fmt.allocPrint(alloc, "foreground-{d}", .{i});
        defer alloc.free(key);
        try live.upsert(alloc, .{ .name = "docs" }, key, "baseline", false);
    }
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    var observed: [4]capacity.AppendCounters = @splat(.{});
    for (0..transitions) |transition| for (0..4) |owner| {
        const txn_id: [16]u8 = @splat(@intCast(owner + 1));
        var incoming: state.ActiveMemTable = .{};
        defer incoming.deinit(alloc);
        for (0..2) |row| {
            var key: [32]u8 = @splat(@intCast(owner + 1));
            key[31] = @intCast(row);
            const value: [64]u8 = @splat(@intCast(transition));
            try incoming.upsert(alloc, .{ .name = "docs" }, &key, &value, false);
        }
        if (transition == 0) {
            const owner_bytes: [record.encoded_bytes]u8 = @splat(0x71);
            try incoming.upsert(alloc, .{ .name = "docs" }, &record.ownerKey(txn_id), &owner_bytes, false);
        } else if (transition == transitions - 1) {
            try incoming.upsert(alloc, .{ .name = "docs" }, &record.ownerKey(txn_id), "", true);
        }
        const identity: @import("completion_runtime.zig").AcceptedIdentity = .{ .term = 3, .index = 1 + transition * 4 + owner, .digest = @splat(0x39) };
        const native_progress = @import("completion_pool.zig").encodeProgress(.{ .capacity = 4, .group_id = 7, .node_id = 9, .incarnation = @splat(11), .policy_digest = @splat(13), .generation = 17 }, identity);
        const progress = try (record.Progress{
            .txn_id = txn_id,
            .begin = .{ .term = 3, .index = 1 + owner, .digest = @splat(0x39) },
            .latest = .{ .term = 3, .index = 1 + transition * 4 + owner, .digest = @splat(0x39) },
            .phase = if (transition == 0) .begin else if (transition == 1) .decision else .acknowledgement,
            .decision = if (transition == 0) .none else .committed,
            .acknowledged = if (transition <= 1) 0 else @intCast(transition - 1),
            .resolved_digest = if (transition <= 1) @splat(0) else @splat(0x71),
        }).encode();
        try incoming.upsert(alloc, .{ .name = "docs" }, &record.progressKey(txn_id), &progress, false);
        try incoming.upsert(alloc, .{ .name = "docs" }, entry.group_progress_key, &native_progress, false);
        try incoming.upsert(alloc, .{ .name = "docs" }, &internal_keys.raft_document_applied_entry_key, identity.encode()[0..16], false);
        observed[owner] = try observed[owner].plus(.{ .bytes = @import("wal.zig").encodedStateRecordLen(&incoming), .entries = incoming.entryCount(), .records = 1 });
        var candidate = try live.preparePublicationOwned(reservations[owner].?.allocator(), &incoming);
        defer candidate.deinit(alloc);
        live.publishPrepared(&candidate);
        readers[transition * 4 + owner] = try live.snapshot(alloc);
    };
    for (0..4) |i| {
        try std.testing.expectEqual(proof.owners[i].append, observed[i]);
        try std.testing.expect(reservations[i].?.remainingBytes() >= proof.owners[i].checkpoint_metadata_bytes);
        reservations[i].?.finish();
        reservations[i] = null;
        domains_owned[i].?.retire();
        domains_owned[i] = null;
    }
    for (&readers, 0..) |*reader, i| {
        const owner = i % 4;
        var key: [32]u8 = @splat(@intCast(owner + 1));
        key[31] = 0;
        const expected: [64]u8 = @splat(@intCast(i / 4));
        try std.testing.expectEqualSlices(u8, &expected, try reader.*.?.get(.{ .name = "docs" }, &key));
    }
    try std.testing.expect(!backing.has_induced_failure);
}
