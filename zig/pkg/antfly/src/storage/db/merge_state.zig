// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the ELv2 at https://www.antfly.io/licensing/ELv2-license

//! Durable receiver-side range-merge state shared by the direct coordinator
//! and data-Raft apply. Keeping one codec is required for leader failover: a
//! follower that applies a replicated checkpoint must be observable by the
//! ordinary MergeCoordinator after promotion.

const std = @import("std");
const db_types = @import("types.zig");
const doc_identity = @import("doc_identity.zig");
const docstore = @import("../docstore.zig");

// Group-owned metadata must sort before document keys so a physical LSM
// split retains it on the parent. Split destinations clear this prefix.
pub const key = "\x00\x00__metadata__:raftmerge";
pub const legacy_key = "raftmerge:state";

pub fn loadRawAlloc(alloc: std.mem.Allocator, store: *docstore.DocStore) !?[]u8 {
    return store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => store.get(alloc, legacy_key) catch |legacy_err| switch (legacy_err) {
            error.NotFound => null,
            else => return legacy_err,
        },
        else => return err,
    };
}

/// Upgrade existing production receipts before a physical split can move or
/// discard the old key. The protected copy and old-key deletion commit in one
/// batch and are synced before the destructive rewrite, not restored after it.
pub fn protectForSplit(alloc: std.mem.Allocator, store: *docstore.DocStore) !void {
    const old = store.get(alloc, legacy_key) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    defer alloc.free(old);
    const current = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (current) |value| alloc.free(value);
    const raw = current orelse old;
    var state = try decodeAlloc(alloc, raw);
    defer state.deinit(alloc);
    try store.putBatch(&.{.{ .key = key, .value = raw }}, &.{legacy_key});
    try store.sync(true);
}

pub const Phase = @import("merge_contract.zig").Phase;
pub const State = @import("merge_contract.zig").State;
pub const encode = @import("merge_contract.zig").encode;
pub const decodeAlloc = @import("merge_contract.zig").decodeAlloc;
pub const ApplyPlan = @import("merge_contract.zig").ApplyPlan;
pub const isRetired = @import("merge_contract.zig").isRetired;
pub const retireCurrentAlloc = @import("merge_contract.zig").retireCurrentAlloc;
pub const copyAllowed = @import("merge_contract.zig").copyAllowed;
pub const planCheckpointApply = @import("merge_contract.zig").planCheckpointApply;
