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

//! Accepting-writer checks shared by bounded catalog and counter journals.
//! A retained result is observed before this guard; an absent, still-admissible
//! request is fenced before effects. No second receipt or coordinator exists.
const std = @import("std");
const local = @import("antfly_local_sources");
const scopes = local.storage_pessimistic_lock_authority;
const protocol = @import("../metadata/topology_protocol.zig");
pub const Fence = scopes.CommandFence;

pub fn check(a: std.mem.Allocator, txn: anytype, group: u64, incarnation: [16]u8, fence: Fence) !void {
    try fence.validate();
    if (!std.mem.eql(u8, &incarnation, &fence.backend.incarnation)) return error.CatalogGenerationChanged;
    var buffer: [160]u8 = undefined;
    const raw = txn.get(try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:topology_activation_v1:{d}", .{group})) catch |err| return switch (err) {
        error.NotFound => error.TableTopologyProtocolUpgradeRequired,
        else => err,
    };
    if (raw.len > protocol.max_activation_bytes) return error.InvalidCatalogRecord;
    var activation = try std.json.parseFromSlice(protocol.Activation, a, raw, .{});
    defer activation.deinit();
    if (activation.value.version < protocol.sql_command_fence_version) return error.TableTopologyProtocolUpgradeRequired;
    if (activation.value.version > protocol.current_version or activation.value.member_count == 0 or
        std.mem.allEqual(u8, &activation.value.membership_fingerprint, 0)) return error.InvalidCatalogRecord;
    var activated_incarnation: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&activated_incarnation, &activation.value.incarnation) catch return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, &activated_incarnation, &incarnation)) return error.CatalogGenerationChanged;
    const standalone = local.common_group_ids.main_metadata_group_id == group and if (txn.get("\x00\x00__metadata__:standalone_catalog")) |_| true else |err| switch (err) {
        error.NotFound => false,
        else => return err,
    };
    if (fence.standalone != standalone) return error.Forbidden;
    const ns: scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = incarnation };
    _ = scopes.guard(a, txn, ns, fence.backend, fence.command, fence) catch |err| return switch (err) {
        error.LockOwnerNotRegistered, error.LockOwnerChanged, error.LockEffectChanged, error.LockEffectOutOfOrder, error.LockEffectExpired => error.CatalogGenerationChanged,
        error.LockOwnerBusy => error.CatalogPublicationProofPending,
        error.InvalidLockRecord => error.InvalidCatalogRecord,
        else => err,
    };
    const owner = (try scopes.ledger.readOwner(a, txn, ns, fence.backend)) orelse return error.InvalidCatalogRecord;
    if (owner.state != .active or owner.waiting != null or owner.job != null) return error.CatalogPublicationProofPending;
}
