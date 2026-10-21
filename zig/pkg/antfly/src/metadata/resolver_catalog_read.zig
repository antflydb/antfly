// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Private, bounded resolver catalog reads. No public request supplies authority.
const std = @import("std");
const metadata = @import("table_manager.zig");
pub const Request = struct {
    table_name: []const u8,
    mode: enum { table, consumers } = .table,
    key: []const u8 = "",
    expected_revision: ?u64 = null,
    cursor: ?[]const u8 = null,
    standalone: bool = false,
};
pub const Page = struct {
    version: u8,
    revision: u64,
    table: metadata.TableRecord,
    ranges: []metadata.RangeRecord = &.{},
    consumers: []const Consumer = &.{},
    visited: usize = 0,
    next_cursor: ?[]const u8 = null,
    indexed: bool = false,
    pub fn snapshot(self: *const Page) @import("api.zig").AdminSnapshot {
        return .{ .status = .{ .metadata_group_id = 0, .metrics = .{} }, .tables = @constCast((&self.table)[0..1]), .ranges = self.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
    }
};
pub const Cursor = struct { revision: u64, table_id: u64, key_digest: [32]u8, indexed: bool, after: []const u8 };
pub fn keyDigest(key: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &out, .{});
    return out;
}

pub const Consumer = struct { table: metadata.TableRecord, names: []const []const u8 };
