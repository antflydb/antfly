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

//! Replicated owners bind leases and source event identities to their logical
//! namespace, not a replica-local filesystem incarnation. Lite has no assigned
//! namespace and uses its persistent root incarnation. Range replacement gets
//! a different namespace; merely changing Raft leaders does not change bytes.
const std = @import("std");
pub fn incarnation(namespace: [24]u8, root: u128) u128 {
    if (std.mem.allEqual(u8, namespace[0..8], 0)) return root;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:group-source-owner:v1");
    hash.update(&namespace);
    const digest = hash.finalResult();
    return std.mem.readInt(u128, digest[0..16], .big) | 1;
}
pub fn fromNamespace(namespace: anytype, root: u128) u128 {
    var raw: [24]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], namespace.table_id, .big);
    std.mem.writeInt(u64, raw[8..16], namespace.shard_id, .big);
    std.mem.writeInt(u64, raw[16..24], namespace.range_id, .big);
    return incarnation(raw, root);
}
test "group owner identity is deterministic across replicas and changes with logical ownership" {
    const one = .{ .table_id = @as(u64, 4), .shard_id = @as(u64, 7), .range_id = @as(u64, 3) };
    try std.testing.expectEqual(fromNamespace(one, 10), fromNamespace(one, 999));
    try std.testing.expect(fromNamespace(one, 10) != fromNamespace(.{ .table_id = @as(u64, 4), .shard_id = @as(u64, 7), .range_id = @as(u64, 4) }, 10));
    try std.testing.expect(incarnation(@splat(0), 10) != incarnation(@splat(0), 999));
}
