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

//! Bounded native topology-control deduplication. One slot per role/action
//! retains the latest fence; the effect, slot and native clock share a txn.

const replication_ingress = @import("replication_ingress.zig");
const std = @import("std");
const topology = @import("relational_integrity_topology_contract.zig");
const position = @import("receipt_position.zig");
const authority = @import("../source_authority.zig");

pub const prefix = "\x00\x00__metadata__:native_topology_receipt:v1:";
const encoded_len = 4 + 136 + 32 + position.Position.encoded_len + 32;

pub fn isKey(candidate: []const u8) bool {
    return std.mem.startsWith(u8, candidate, prefix);
}

fn key(command: topology.Command) [prefix.len + 2]u8 {
    var result: [prefix.len + 2]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    result[prefix.len] = @intCast(@intFromEnum(command.fence.role));
    result[prefix.len + 1] = @intCast(@intFromEnum(command.action));
    return result;
}

pub const Prepared = struct { receipt: position.Native, duplicate: bool };

pub fn supports(command: topology.Command) bool {
    return switch (command.fence.role) {
        .rewrite_source => switch (command.action) {
            .begin, .cancel, .abort_transition, .seal_graph_retirement, .seal_generation_handoff => true,
            else => false,
        },
        .rewrite_destination => command.action == .install_generation_handoff,
        .truncate_parent => switch (command.action) {
            .begin, .cancel, .stage_parent_retirement, .activate_parent_retirement, .acknowledge_parent_retirement => true,
            else => false,
        },
        else => false,
    };
}

/// Native receipt envelopes cannot smuggle unrelated effects. Comparing the
/// canonical request with its control-only projection also covers new fields.
pub fn validateRequest(alloc: std.mem.Allocator, request: @import("types.zig").BatchRequest) !void {
    const command = request.relational_topology orelse return error.InvalidBatchRequest;
    if (!supports(command)) return error.InvalidBatchRequest;
    const expected: @import("types.zig").BatchRequest = .{
        .relational_topology = command,
        .restore_staging_scope = request.restore_staging_scope,
        .restore_staging_plan_id = request.restore_staging_plan_id,
        .sync_level = request.sync_level,
    };
    const actual_bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(actual_bytes);
    const expected_bytes = try std.json.Stringify.valueAlloc(alloc, expected, .{});
    defer alloc.free(expected_bytes);
    if (!std.mem.eql(u8, actual_bytes, expected_bytes)) return error.InvalidBatchRequest;
}

pub fn stage(alloc: std.mem.Allocator, txn: anytype, command: topology.Command, replay: ?position.Native) !Prepared {
    if (!supports(command)) return error.InvalidBatchRequest;
    const namespace = @import("online_source_contract.zig").namespaceBytes(command.fence.namespace);
    const owner = try authority.require(txn, .native, namespace);
    if (replay) |value| {
        try value.validate();
        if (!value.namespace.eql(command.fence.namespace)) return error.IdentityNamespaceMismatch;
    }
    const serialized = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(serialized);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(serialized, &digest, .{});
    const slot = key(command);
    const previous = txn.get(&slot) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (previous) |bytes| {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "NTR1")) return error.InvalidControlReceiptPosition;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. encoded_len - 32], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[encoded_len - 32 ..])) return error.InvalidControlReceiptPosition;
        const fence = try topology.Fence.decode(bytes[4..140]);
        const stamp = try position.Position.decode(bytes[172..205]);
        if (stamp != .native) return error.InvalidControlReceiptPosition;
        try stamp.requireNamespace(fence.namespace);
        // Authenticated generation adoption binds a new durable namespace and
        // resets its native clock. Copied prior-incarnation slots are inert;
        // they cannot authorize retries or constrain the new owner's epochs.
        if (fence.namespace.eql(command.fence.namespace)) {
            if (owner.sequence < stamp.native.sequence) return error.InvalidControlReceiptPosition;
            if (fence.eql(command.fence)) {
                if (!std.mem.eql(u8, &digest, bytes[140..172])) return error.IntegrityTopologyChanged;
                if (replay) |value| if (!std.meta.eql(value, stamp.native)) return error.InvalidControlReceiptPosition;
                return .{ .receipt = stamp.native, .duplicate = true };
            }
            if (command.fence.admission_epoch <= fence.admission_epoch) return error.IntegrityTopologyChanged;
        }
    }
    const sequence = try authority.advance(txn, namespace, if (replay) |value| value.sequence else null);
    const receipt: position.Native = .{ .namespace = command.fence.namespace, .sequence = sequence };
    var bytes: [encoded_len]u8 = undefined;
    @memcpy(bytes[0..4], "NTR1");
    @memcpy(bytes[4..140], &try command.fence.encode());
    @memcpy(bytes[140..172], &digest);
    @memcpy(bytes[172..205], &try (position.Position{ .native = receipt }).encode());
    std.crypto.hash.Blake3.hash(bytes[0..205], bytes[205..237], .{});
    try txn.put(&slot, &bytes);
    return .{ .receipt = receipt, .duplicate = false };
}

const fixture_owner = @This();
pub const test_support = if (@import("builtin").is_test) struct {
    pub const key = fixture_owner.key;
} else struct {};
