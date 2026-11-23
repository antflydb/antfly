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

//! Bounded internal acceptance receipts. Native checkpoint evidence proves
//! acceptance only; completion, source ACK and reclamation remain separate.
const std = @import("std");
const local = @import("antfly_local_sources");
const source = @import("../metadata/source_control.zig");
const destination = @import("source_destination_plan.zig");
const admission = @import("source_destination_admission.zig");
const checkpoints = @import("source_checkpoint_operations.zig");
const A = std.mem.Allocator;
pub const max_bytes = 8192;
pub const Receipt = struct {
    version: u8 = 1,
    reservation_digest: [32]u8,
    manifest_digest: [32]u8,
    source_scope: local.storage_source_scope.Reference,
    checkpoint: checkpoints.wire.Reply,
};
pub const Retained = struct {
    receipt: Receipt,
    manifest: std.json.Parsed(destination.Manifest),
};
pub const Bound = union(enum) {
    retained: Retained,
    /// Exact metadata reference and last-reclaimed proof agree. No native
    /// authority is reconstructed after its recovery manifest was reclaimed.
    reclaimed: void,
    pub fn deinit(self: *Bound) void {
        switch (self.*) {
            .retained => |*value| value.manifest.deinit(),
            .reclaimed => {},
        }
        self.* = undefined;
    }
};
fn query(manifest: destination.Manifest) checkpoints.wire.Request {
    return .{ .version = 1, .source_owner = manifest.owner, .transaction_id = manifest.transaction_id, .command = manifest.checkpoint, .minimum_applied_index = 1 };
}
fn requireState(state: source.State) !void {
    try state.validate();
    if (!state.scope_fencing or !state.manifest_retention or state.scope == null or state.scope.?.destination != .native) return error.UnsupportedOperation;
}
fn manifestFor(a: A, state: source.State) !std.json.Parsed(destination.Manifest) {
    try requireState(state);
    const record = state.artifact_control.?.current orelse return error.SourcePendingWork;
    if (record.phase == .reserved) return error.SourcePendingWork;
    const saved = record.recovery orelse return error.SourcePendingWork;
    var parsed = if (state.pending) |pending| blk: {
        const grant = try admission.Grant.init(a, state.scope.?, pending, record);
        break :blk grant.manifest;
    } else try destination.parse(a, saved.bytes, saved.digest);
    errdefer parsed.deinit();
    const manifest = parsed.value;
    const plan = try manifest.deliveryPlan(a, saved.bytes);
    const expected = (try @import("../metadata/source_scope_binding.zig").spec(state)).?;
    if (!std.meta.eql(plan.artifact.?, record.spec) or !state.checkpoint_authority.?.sameAuthority(manifest.checkpoint_authority) or
        !std.mem.eql(u8, &expected.binding, &manifest.source_scope.binding) or !std.mem.eql(u8, &expected.domain(), &manifest.source_scope.domain) or expected.generation != manifest.source_scope.generation) return error.SourceTransactionIdentityConflict;
    return parsed;
}
pub fn accept(a: A, state: source.State, native: admission.Accepted) !@import("../metadata/source_delivery.zig").Acceptance {
    var manifest = try manifestFor(a, state);
    defer manifest.deinit();
    const record = state.artifact_control.?.current.?;
    if (!std.mem.eql(u8, &record.reservation, &native.reservation_digest)) return error.SourceTransactionIdentityConflict;
    try native.checkpoint.validate(query(manifest.value));
    if (native.checkpoint.disposition != .committed) return error.SourceOutcomeUnresolved;
    const receipt: Receipt = .{ .reservation_digest = record.reservation, .manifest_digest = record.recovery.?.digest, .source_scope = manifest.value.source_scope, .checkpoint = native.checkpoint };
    const encoded = try std.json.Stringify.valueAlloc(a, receipt, .{});
    errdefer a.free(encoded);
    if (encoded.len > max_bytes) return error.InvalidSourceControl;
    // Checkpoint acceptance never marks metadata's committed frontier here.
    return .{ .reservation_digest = receipt.reservation_digest, .receipt = encoded };
}
pub fn decode(a: A, state: source.State, encoded: []const u8) !Bound {
    if (encoded.len == 0 or encoded.len > max_bytes) return error.InvalidSourceControl;
    const Raw = struct { version: u8, reservation_digest: [32]u8, manifest_digest: [32]u8, source_scope: local.storage_source_scope.Reference, checkpoint: std.json.Value };
    var raw = std.json.parseFromSlice(Raw, a, encoded, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceControl;
    defer raw.deinit();
    try requireState(state);
    if (raw.value.version != 1 or std.mem.allEqual(u8, &raw.value.reservation_digest, 0) or std.mem.allEqual(u8, &raw.value.manifest_digest, 0)) return error.InvalidSourceControl;
    try raw.value.source_scope.validate();
    const expected = (try @import("../metadata/source_scope_binding.zig").spec(state)).?;
    if (raw.value.source_scope.generation != expected.generation or !std.mem.eql(u8, &raw.value.source_scope.binding, &expected.binding) or !std.mem.eql(u8, &raw.value.source_scope.domain, &expected.domain())) return error.SourceTransactionIdentityConflict;
    const ledger = state.artifact_control.?;
    // The single current upload cannot start before its predecessor's proven
    // reclamation. This cumulative floor remains useful after newer attempts
    // replace the last-reclaimed identity or own the current manifest.
    if (ledger.last_reclaimed) |last| {
        if (raw.value.source_scope.token <= last.spec.token() and raw.value.source_scope.sequence == state.accepted.sequence and std.mem.eql(u8, encoded, state.accepted.reference) and std.mem.eql(u8, &raw.value.reservation_digest, &state.accepted.reservation_digest)) return .reclaimed;
    }
    if (state.pending == null and !std.mem.eql(u8, encoded, state.accepted.reference)) return error.SourceTransactionIdentityConflict;
    const record = ledger.current orelse return error.SourceTransactionIdentityConflict;
    if (!std.mem.eql(u8, &record.reservation, &raw.value.reservation_digest) or record.recovery == null or !std.mem.eql(u8, &record.recovery.?.digest, &raw.value.manifest_digest)) return error.SourceTransactionIdentityConflict;
    var manifest = try manifestFor(a, state);
    errdefer manifest.deinit();
    const checkpoint_json = try std.json.Stringify.valueAlloc(a, raw.value.checkpoint, .{});
    defer a.free(checkpoint_json);
    if (!std.meta.eql(raw.value.source_scope, manifest.value.source_scope)) return error.SourceTransactionIdentityConflict;
    const observed = try checkpoints.wire.decodeReply(a, query(manifest.value), checkpoint_json);
    if (observed.disposition != .committed) return error.SourceOutcomeUnresolved;
    return .{ .retained = .{ .manifest = manifest, .receipt = .{ .reservation_digest = raw.value.reservation_digest, .manifest_digest = raw.value.manifest_digest, .source_scope = raw.value.source_scope, .checkpoint = observed } } };
}
