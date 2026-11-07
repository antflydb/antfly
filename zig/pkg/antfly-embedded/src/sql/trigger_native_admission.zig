// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native receiver verification for statement-prepared trigger effects. HTTP
//! transport and the compiled owner use distinct, body-bound grant purposes.
//! Only ordered owner replay uses the admitted instant from its committed
//! command; live requests always use the receiver clock. Programs never run
//! here, including transaction retry, recovery and hot standby replay.
const std = @import("std");
const types = @import("../storage/db/types.zig");
const authority = @import("../common/trigger_preparation_authority.zig");
const handoff = @import("trigger_handoff.zig");
const A = std.mem.Allocator;

pub fn scope(a: A, req: types.BatchRequest) !authority.Scope {
    if (req.trigger_receipts.len == 0 or req.trigger_preparation_group_id == 0 or req.trigger_preparation_admitted_at_seconds <= 0) return error.SqlTriggerCoordinationRequired;
    const table = req.trigger_receipts[0].program.table;
    for (req.trigger_receipts) |receipt| if (!receipt.program.table.eql(table)) return error.InvalidSqlTriggerManifest;
    var unsigned = req;
    unsigned.trigger_preparation_proof = "";
    // Time and topology are independently materialized/fenced by the owner.
    // Transaction identity and every logical effect remain in the signed body.
    unsigned.timestamp_ns = 0;
    if (unsigned.transaction) |*command| if (command.* == .prepare) {
        command.prepare.topology_epoch = 0;
    };
    const body = try @import("../api/batch.zig").encodeOwnerBatchRequest(a, unsigned);
    defer a.free(body);
    return .{
        .purpose = if (req.transaction != null and req.transaction.? == .prepare) .native_prepare else .native_batch,
        .table_id = table.id,
        .group_id = req.trigger_preparation_group_id,
        .table_fingerprint = try table.fingerprint(),
        .receipts_digest = try handoff.receiptsDigest(a, req.trigger_receipts),
        .body_digest = authority.bodyDigest(body),
        .command_fence_digest = req.trigger_preparation_command_digest,
    };
}
pub fn seal(a: A, minter: authority.Minter, group_id: u64, now: i64, req: types.BatchRequest) !types.BatchRequest {
    if (req.trigger_receipts.len == 0) return req;
    var prepared = req;
    prepared.trigger_preparation_group_id = group_id;
    prepared.trigger_preparation_command_digest = minter.command_fence_digest;
    prepared.trigger_preparation_admitted_at_seconds = now;
    prepared.trigger_preparation_proof = try minter.mint(a, try scope(a, prepared));
    return prepared;
}
/// The group is supplied by the actual compiled owner handle, never inferred
/// from its shard/range ID (those IDs need not equal the Raft group).
pub fn requireOwnerGroup(req: types.BatchRequest, actual_group_id: u64) !void {
    if (req.trigger_receipts.len != 0 and (actual_group_id == 0 or req.trigger_preparation_group_id != actual_group_id)) return error.PreparedGenerationChanged;
}
/// Absence is an epoch mismatch, not permission to forget prepared effects.
/// In particular a reused native group cannot apply an old table's signed
/// rows to a newly opened owner that has no published schema at all.
pub fn requireSchemaView(req: types.BatchRequest, present: bool) !void {
    if (!present) for (req.trigger_receipts) |receipt| {
        const absent = receipt.program.absence orelse return error.CatalogGenerationChanged;
        try absent.requireNoSchema();
    };
}
pub fn requireOwnerTable(req: types.BatchRequest, actual_table_id: u64) !void {
    if (req.graph_preparation) |proof| {
        try proof.validate();
        if (proof.table_id != actual_table_id or req.trigger_receipts.len == 0) return error.CatalogGenerationChanged;
    }
    for (req.trigger_receipts) |receipt| if (actual_table_id == 0 or receipt.program.table.id != actual_table_id) return error.CatalogGenerationChanged;
}
pub fn verify(a: A, view: anytype, req: types.BatchRequest, secret: ?[]const u8, issuer: ?[]const u8, now: i64, trusted_replay: bool) !void {
    if (req.transforms.len != 0) {
        const required = if (view.triggerState()) |state| state.entries.len != 0 else false;
        if (required or req.trigger_receipts.len != 0) return error.SqlTriggerCoordinationRequired;
    }
    for (req.trigger_receipts) |receipt| if (receipt.program.absence) |absent| {
        try absent.requireAdmission(req.graph_preparation orelse return error.SqlTriggerCoordinationRequired);
        try absent.requireView(view);
    };
    if (req.trigger_receipts.len == 0) return handoff.verifyPrepared(a, view, req.trigger_receipts, false, req.writes, req.deletes, req.predicates);
    if (req.trigger_preparation_proof.len == 0) return error.SqlTriggerCoordinationRequired;
    try verifyEnvelope(a, req, secret, issuer, now, trusted_replay);
    // The caller can set trusted_replay only after authenticating an ordered
    // owner entry. Its immutable effects must remain recoverable after user
    // key rotation, expiry or loss of the original API process; a replay is
    // not a new live user delegation and never executes a trigger again.
    try handoff.verifyPrepared(a, view, req.trigger_receipts, true, req.writes, req.deletes, req.predicates);
}

fn verifyEnvelope(a: A, req: types.BatchRequest, secret: ?[]const u8, issuer: ?[]const u8, now: i64, trusted_replay: bool) !void {
    // A command fence was proved by the API before minting this native grant.
    // It remains bound inside the token, while immutable table/rows and the
    // exact transaction body are checked again by the native receiver.
    if (!trusted_replay) {
        _ = try authority.verify(a, secret orelse return error.SqlTriggerContextRequired, issuer orelse return error.SqlTriggerContextRequired, try scope(a, req), now, req.trigger_preparation_proof);
    }
}

pub fn verifyNoSchema(a: A, req: types.BatchRequest, secret: ?[]const u8, issuer: ?[]const u8, now: i64, trusted_replay: bool) !void {
    try requireSchemaView(req, false);
    if (req.trigger_receipts.len == 0) return;
    if (req.schema_version != null or req.relational_schema_version != null or req.transforms.len != 0 or req.trigger_preparation_proof.len == 0) return error.SqlTriggerCoordinationRequired;
    const proof = req.graph_preparation orelse return error.SqlTriggerCoordinationRequired;
    try proof.validate();
    for (req.trigger_receipts) |receipt| {
        const absent = receipt.program.absence orelse return error.CatalogGenerationChanged;
        try absent.requireNoSchema();
        try absent.requireAdmission(proof);
    }
    try verifyEnvelope(a, req, secret, issuer, now, trusted_replay);
    try handoff.verifyPreparedNoSchema(a, req.trigger_receipts, true, req.writes, req.deletes, req.predicates);
}
