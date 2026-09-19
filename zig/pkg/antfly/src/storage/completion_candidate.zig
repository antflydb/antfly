// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Leader-side candidate assembly. This owns no accepted resources and grants
//! no authority: native admission still checks its baseline and takes backing
//! before the consensus boundary. All reads use the caller's stable snapshot.
const std = @import("std");
const compiler = @import("completion_compiler.zig");
const codec = @import("lsm_backend/completion_entry.zig");
const slot = @import("lsm_backend/completion_slot.zig");
const erased = @import("backend_erased.zig");
const Allocator = std.mem.Allocator;
const docstore = @import("docstore.zig");
const mutations = @import("completion_mutations.zig");

pub const Authority = struct {
    group_id: u64,
    incarnation: [16]u8,
    policy_digest: [32]u8,
    schema_catalog_digest: [32]u8,
    previous_term: u64,
    previous_index: u64,
};

/// This identity belongs to a single accepted physical mutation, not a logical
/// transaction. The domain and full authority prevent ordinary writes from
/// borrowing a transaction's completion ownership by copying its request ID.
pub fn mutationId(authority: Authority, input_digest: [32]u8) [16]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-completion-mutation-id-v1\x00");
    var numbers: [24]u8 = undefined;
    std.mem.writeInt(u64, numbers[0..8], authority.group_id, .little);
    std.mem.writeInt(u64, numbers[8..16], authority.previous_term, .little);
    std.mem.writeInt(u64, numbers[16..24], authority.previous_index, .little);
    hash.update(&numbers);
    hash.update(&authority.incarnation);
    hash.update(&authority.policy_digest);
    hash.update(&authority.schema_catalog_digest);
    hash.update(&input_digest);
    return hash.finalResult()[0..16].*;
}

/// Expand the real final DB batch through DocStore's physical writer. The
/// overlay exposes only reads of a stable snapshot: speculative compilation
/// cannot publish a document, columnar token, replay record, or transaction.
/// No future outcome templates or dynamic bindings belong to this entry.
pub fn encodePhysicalMutation(
    alloc: Allocator,
    authority: Authority,
    input_digest: [32]u8,
    profile_fence: []const u8,
    limits: slot.Limits,
    snapshot: *erased.ReadTxn,
    writes: []const docstore.KVPair,
    deletes: []const []const u8,
    replay: ?docstore.DocStore.ReplayAppend,
    additional_dependencies: []const []const u8,
) ![]u8 {
    const plan_limits: mutations.Limits = .{ .max_operations = codec.max_operations, .max_bytes = codec.max_descriptor_bytes };
    var overlay = try compiler.Overlay.init(alloc, snapshot, plan_limits);
    defer overlay.deinit();
    var store = overlay.store();
    var baseline = try store.beginBatch();
    defer baseline.abort();
    var plan = try mutations.Plan.init(alloc, plan_limits);
    defer plan.deinit();
    var physical: compiler.PhysicalSink = .{ .baseline = &baseline, .plan = &plan };
    var runtime = physical.runtime(alloc);
    const writer = physical.writer(alloc, &runtime);
    for (deletes) |key| try writer.delete(key);
    for (writes) |write| try writer.put(write.key, write.value);
    if (replay) |entry| try writer.setReplayOpaque(entry.sequence, entry.payload);
    const operations = try alloc.alloc(slot.Operation, plan.count);
    defer alloc.free(operations);
    for (plan.operations(), operations) |op, *out| out.* = .{
        .kind = if (op.kind == .put) .put else .delete,
        .key = op.key,
        .value = op.value,
    };
    var keys: std.ArrayListUnmanaged([]const u8) = .empty;
    defer keys.deinit(alloc);
    for (overlay.baseline_reads.items) |key| try addKey(alloc, &keys, key);
    for (additional_dependencies) |key| try addKey(alloc, &keys, key);
    for (operations) |op| try addKey(alloc, &keys, op.key);
    std.mem.sort([]const u8, keys.items, {}, lessThan);
    var baseline_hash = codec.BaselineHasher.init();
    for (keys.items) |key| {
        const value = snapshot.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        try baseline_hash.add(key, value);
    }
    const id = mutationId(authority, input_digest);
    const descriptor = try slot.encode(alloc, .{
        .txn_id = id,
        .intent_revision = 0,
        .namespace = "docs",
        .limits = limits,
        .profile_fence = profile_fence,
        .commit = &.{},
        .abort = &.{},
    }, .{});
    defer alloc.free(descriptor);
    return codec.encode(alloc, .{
        .kind = .mutation,
        .group_id = authority.group_id,
        .group_incarnation = authority.incarnation,
        .policy_digest = authority.policy_digest,
        .schema_catalog_digest = authority.schema_catalog_digest,
        .previous_term = authority.previous_term,
        .previous_index = authority.previous_index,
        .txn_id = id,
        .original_input_digest = input_digest,
        .baseline_digest = baseline_hash.finish(),
        .baseline_keys = keys.items,
        .descriptor = descriptor,
        .prepare_operations = operations,
    });
}

pub fn encode(
    alloc: Allocator,
    authority: Authority,
    txn_id: [16]u8,
    input_digest: [32]u8,
    descriptor: []const u8,
    templates: *const compiler.CompiledTemplates,
    snapshot: *erased.ReadTxn,
    additional_dependencies: []const []const u8,
) ![]u8 {
    var keys: std.ArrayListUnmanaged([]const u8) = .empty;
    defer keys.deinit(alloc);
    for (templates.baseline_reads) |key| try addKey(alloc, &keys, key);
    for (additional_dependencies) |key| try addKey(alloc, &keys, key);
    for (templates.prepare.operations) |op| try addKey(alloc, &keys, op.key);
    for ([_][]const slot.Operation{ templates.commit.operations, templates.abort.operations }) |operations| {
        for (operations) |op| if (!slot.isSharedDynamicOperation(op)) try addKey(alloc, &keys, op.key);
    }
    std.mem.sort([]const u8, keys.items, {}, lessThan);
    var baseline = codec.BaselineHasher.init();
    for (keys.items) |key| {
        const value = snapshot.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        try baseline.add(key, value);
    }
    // The prepare is an exact canonical delta. Dynamic fields belong only to
    // future commit/abort, whose explicit annotations remain in descriptor.
    const prepare = try alloc.alloc(slot.Operation, templates.prepare.operations.len);
    defer alloc.free(prepare);
    for (templates.prepare.operations, prepare) |op, *out| {
        out.* = op;
        out.bindings = &.{};
    }
    return codec.encode(alloc, .{
        .group_id = authority.group_id,
        .group_incarnation = authority.incarnation,
        .policy_digest = authority.policy_digest,
        .schema_catalog_digest = authority.schema_catalog_digest,
        .previous_term = authority.previous_term,
        .previous_index = authority.previous_index,
        .txn_id = txn_id,
        .original_input_digest = input_digest,
        .baseline_digest = baseline.finish(),
        .baseline_keys = keys.items,
        .descriptor = descriptor,
        .prepare_operations = prepare,
    });
}

fn lessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}
fn addKey(alloc: Allocator, keys: *std.ArrayListUnmanaged([]const u8), key: []const u8) !void {
    for (keys.items) |existing| if (std.mem.eql(u8, existing, key)) return;
    if (keys.items.len == codec.max_baseline_keys) return error.CompletionPlanCapacityExceeded;
    try keys.append(alloc, key);
}
