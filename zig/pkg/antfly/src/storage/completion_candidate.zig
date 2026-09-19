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

pub const Authority = struct {
    group_id: u64,
    incarnation: [16]u8,
    policy_digest: [32]u8,
    schema_catalog_digest: [32]u8,
    previous_term: u64,
    previous_index: u64,
};

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
