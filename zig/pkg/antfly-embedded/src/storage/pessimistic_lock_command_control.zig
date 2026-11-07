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

//! Assign one savepoint effect in the accepting writer and retain that exact
//! assignment across response loss. One bounded record per participant command
//! avoids guessing next_effect from a later observation after completion.
const std = @import("std");
const scopes = @import("pessimistic_lock_authority.zig");
const participant = @import("pessimistic_lock_participant.zig");
const ledger = scopes.ledger;
const A = std.mem.Allocator;
pub const Request = struct {
    nonce: [16]u8,
    control: ledger.Control,
    failed_root: bool = false,
    pub fn validate(self: Request, admission: participant.Admission) !void {
        try (participant.SavepointControl{ .admission = admission, .failed_root = self.failed_root, .effect = .{ .backend = (try admission.registration()).lease.backend, .sequence = 1, .nonce = self.nonce, .action = .{ .control = self.control } } }).validate();
    }
};
const Record = struct { command: scopes.Command, effect: ledger.Effect };
fn address(buffer: []u8, admission: participant.Admission) ![]const u8 {
    const ns = admission.binding.namespace;
    const backend = (try admission.registration()).lease.backend;
    return std.fmt.bufPrint(buffer, "\x00\x00__lock_command_control__:{x:0>16}:{s}:{s}:{x:0>16}", .{ ns.group, std.fmt.bytesToHex(ns.incarnation, .lower), std.fmt.bytesToHex(backend.id, .lower), backend.fence });
}
fn read(a: A, txn: anytype, admission: participant.Admission) !?Record {
    var buffer: [224]u8 = undefined;
    const bytes = txn.get(try address(&buffer, admission)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len <= 32 or bytes.len > 2048) return error.InvalidLockRecord;
    const body = bytes[0 .. bytes.len - 32];
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(body, &checksum, .{});
    if (!std.mem.eql(u8, &checksum, bytes[body.len..])) return error.InvalidLockRecord;
    const result = try std.json.parseFromSliceLeaky(Record, a, body, .{});
    try result.command.validate();
    try result.effect.validate();
    if (!result.effect.backend.eql((try admission.registration()).lease.backend) or result.effect.action != .control) return error.InvalidLockRecord;
    return result;
}
pub fn effect(a: A, txn: anytype, admission: participant.Admission) !?ledger.Effect {
    const previous = try read(a, txn, admission) orelse return null;
    return if (std.meta.eql(previous.command, try admission.command())) previous.effect else null;
}
pub fn apply(a: A, txn: anytype, receiving: @import("db/doc_identity_namespace.zig").Namespace, admission: participant.Admission, request: Request, authority: anytype) !ledger.Receipt {
    try request.validate(admission);
    try admission.binding.requireIdentity(receiving);
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const command = try admission.command();
    const previous = try read(scratch, txn, admission);
    const selected: ledger.Effect = if (previous != null and std.meta.eql(previous.?.command, command)) replay: {
        const known = previous.?.effect;
        if (!std.mem.eql(u8, &request.nonce, &known.nonce) or !std.meta.eql(request.control, known.action.control)) return error.LockEffectChanged;
        break :replay known;
    } else fresh: {
        if (previous) |known| if (known.command.sequence >= command.sequence) return error.LockEffectExpired;
        const owner = try ledger.readOwner(scratch, txn, admission.binding.namespace, (try admission.registration()).lease.backend) orelse return error.LockOwnerNotRegistered;
        break :fresh .{ .backend = owner.lease.backend, .sequence = owner.next_effect, .nonce = request.nonce, .action = .{ .control = request.control } };
    };
    const receipt = try participant.controlStep(a, txn, receiving, .{ .admission = admission, .effect = selected, .failed_root = request.failed_root }, authority);
    const body = try std.json.Stringify.valueAlloc(scratch, Record{ .command = command, .effect = selected }, .{});
    if (body.len + 32 > 2048) return error.InvalidLockRecord;
    const bytes = try scratch.alloc(u8, body.len + 32);
    @memcpy(bytes[0..body.len], body);
    std.crypto.hash.Blake3.hash(body, bytes[body.len..][0..32], .{});
    var buffer: [224]u8 = undefined;
    try txn.put(try address(&buffer, admission), bytes);
    return receipt;
}
