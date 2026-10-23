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

//! Deterministic storage-owner open contract carried by replicated apply.
//! This module intentionally contains no DB, catalog, or runtime imports.

pub const Identity = struct {
    table_id: u64,
    shard_id: u64,
    range_id: u64,

    pub fn eql(left: Identity, right: Identity) bool {
        return left.table_id == right.table_id and
            left.shard_id == right.shard_id and
            left.range_id == right.range_id;
    }
};

pub const Descriptor = struct {
    lsm_root_generation: u64,
    identity: Identity,
    schema_json: []const u8 = "",
    indexes_json: []const u8 = "",
    /// Authenticated first-open hint, not an instruction to replace a durable
    /// range subsequently changed by replicated split/merge commands.
    initial_range: ?@import("antfly_local_sources").storage_byte_range.ByteRange = null,
    /// Immutable, privately authenticated restore authority. Never populated
    /// from the public catalog or a client-supplied table definition.
    restore_bootstrap_json: []const u8 = "",
    initial_child_bootstrap_json: []const u8 = "",
    restore_cancel_recovery: bool = false,
    restore_ha_replay: bool = false,
    table_storage: ?@import("antfly_local_sources").common_table_storage.Settings = null,
    restore: ?@import("restore_identity.zig").Identity = null,
};

pub fn cloneInitialRange(alloc: @import("std").mem.Allocator, range: ?@import("antfly_local_sources").storage_byte_range.ByteRange) !?@import("antfly_local_sources").storage_byte_range.ByteRange {
    const value = range orelse return null;
    const start = try alloc.dupe(u8, value.start);
    errdefer alloc.free(start);
    return .{ .start = start, .end = try alloc.dupe(u8, value.end) };
}

pub fn freeInitialRange(alloc: @import("std").mem.Allocator, range: ?@import("antfly_local_sources").storage_byte_range.ByteRange) void {
    if (range) |value| {
        alloc.free(value.start);
        alloc.free(value.end);
    }
}

pub fn initialRangesEqual(left: ?@import("antfly_local_sources").storage_byte_range.ByteRange, right: ?@import("antfly_local_sources").storage_byte_range.ByteRange) bool {
    if (left == null or right == null) return left == null and right == null;
    return @import("std").mem.eql(u8, left.?.start, right.?.start) and @import("std").mem.eql(u8, left.?.end, right.?.end);
}
