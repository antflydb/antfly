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

pub const std = @import("std");
pub const contract = @import("distributed_txn_contract.zig");
pub const table_participant_v2_prefix = "table2:";
pub const TableCommitRequest = contract.TableCommitRequest;
pub const CommitConflict = contract.CommitConflict;
pub const CommitOutcome = contract.CommitOutcome;
pub const PreDecisionContext = contract.PreDecisionContext;
pub fn participantIdForGroup(alloc: std.mem.Allocator, table_name: []const u8, group_id: u64) ![]u8 {
    if (table_name.len > std.math.maxInt(u32)) return error.TableNameTooLong;
    return try std.fmt.allocPrint(alloc, "{s}{x:0>8}:{s}:{d}", .{ table_participant_v2_prefix, table_name.len, table_name, group_id });
}

pub const table_participant_prefix = "table:";
pub const table_participant_v3_prefix = "table3:";
pub const group_participant_marker = ":group:";

pub const ParticipantRef = struct {
    table_name: []const u8,
    group_id: u64,
    restore_staging_scope: ?[32]u8 = null,
    restore_staging_plan_id: ?[16]u8 = null,
};

/// The existing durable participant set owns recovery routing. Hidden owners
/// add a fixed-size exact locator, so restart never depends on a resident cache
/// or a scan through all restore jobs. Ordinary participant IDs are unchanged.
pub fn participantIdForGroupScoped(alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, scope: ?[32]u8, plan_id: ?[16]u8) ![]u8 {
    if (scope == null and plan_id == null) return participantIdForGroup(alloc, table_name, group_id);
    try validateRestorePlan(scope, plan_id);
    if (scope == null or plan_id == null or table_name.len == 0 or table_name.len > std.math.maxInt(u32) or group_id == 0) return error.InvalidTxnRequest;
    return std.fmt.allocPrint(alloc, "{s}{x:0>8}:{s}:{d}:{s}:{s}", .{ table_participant_v3_prefix, table_name.len, table_name, group_id, std.fmt.bytesToHex(plan_id.?, .lower), std.fmt.bytesToHex(scope.?, .lower) });
}

pub fn parseParticipantRef(participant: []const u8) ?ParticipantRef {
    if (std.mem.startsWith(u8, participant, table_participant_v3_prefix)) {
        const body = participant[table_participant_v3_prefix.len..];
        if (body.len < 9 or body[8] != ':') return null;
        const table_name_len = std.fmt.parseUnsigned(u32, body[0..8], 16) catch return null;
        if (table_name_len == 0 or table_name_len > body.len - 9) return null;
        const group_separator = 9 + @as(usize, table_name_len);
        if (group_separator >= body.len or body[group_separator] != ':') return null;
        const suffix = body[group_separator + 1 ..];
        const group_end = std.mem.indexOfScalar(u8, suffix, ':') orelse return null;
        if (suffix.len - group_end != 1 + 32 + 1 + 64 or suffix[group_end + 33] != ':') return null;
        const group_id = std.fmt.parseUnsigned(u64, suffix[0..group_end], 10) catch return null;
        if (group_id == 0) return null;
        var plan: [16]u8 = undefined;
        var scope: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&plan, suffix[group_end + 1 ..][0..32]) catch return null;
        _ = std.fmt.hexToBytes(&scope, suffix[group_end + 34 ..]) catch return null;
        if (std.mem.allEqual(u8, &plan, 0)) return null;
        return .{ .table_name = body[9..group_separator], .group_id = group_id, .restore_staging_scope = scope, .restore_staging_plan_id = plan };
    }
    if (std.mem.startsWith(u8, participant, table_participant_v2_prefix)) {
        const body = participant[table_participant_v2_prefix.len..];
        if (body.len < 9 or body[8] != ':') return null;
        const table_name_len = std.fmt.parseUnsigned(u32, body[0..8], 16) catch return null;
        const table_start: usize = 9;
        const group_separator = table_start + @as(usize, table_name_len);
        if (body.len <= group_separator or body[group_separator] != ':') return null;
        const table_name = body[table_start..group_separator];
        if (table_name.len == 0) return null;
        const group_id = std.fmt.parseUnsigned(u64, body[group_separator + 1 ..], 10) catch return null;
        return .{ .table_name = table_name, .group_id = group_id };
    }

    if (!std.mem.startsWith(u8, participant, table_participant_prefix)) return null;
    const rest = participant[table_participant_prefix.len..];
    const marker_index = std.mem.indexOf(u8, rest, group_participant_marker) orelse return null;
    const table_name = rest[0..marker_index];
    if (table_name.len == 0) return null;
    const group_id = std.fmt.parseUnsigned(u64, rest[marker_index + group_participant_marker.len ..], 10) catch return null;
    return .{ .table_name = table_name, .group_id = group_id };
}

pub fn validateRestorePlan(scope: ?[32]u8, plan_id: ?[16]u8) !void {
    if (plan_id) |id| if (scope == null or std.mem.allEqual(u8, &id, 0)) return error.InvalidTxnRequest;
}
